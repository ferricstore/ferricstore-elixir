defmodule FerricStore.SDK.Native.ConnectionRequest do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  alias FerricStore.SDK.Native.{
    ConnectionDrain,
    ConnectionPending,
    ConnectionTimers
  }

  @type target :: {:call, GenServer.from()} | {:message, pid(), reference()} | :heartbeat

  @spec submit(map(), target(), non_neg_integer(), term(), non_neg_integer(), timeout()) ::
          {:ok, map()} | {:error, term(), map()}
  def submit(state, target, opcode, payload, lane_id, timeout) do
    submit(
      state,
      target,
      opcode,
      payload,
      lane_id,
      timeout,
      ConnectionTimers.request_deadline(timeout)
    )
  end

  @spec submit(
          map(),
          target(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout(),
          integer() | :infinity
        ) :: {:ok, map()} | {:error, term(), map()}
  def submit(
        %{drain: %{active: true}} = state,
        _target,
        _opcode,
        _payload,
        _lane,
        _timeout,
        _deadline
      ),
      do: {:error, :connection_draining, state}

  def submit(state, target, opcode, payload, lane_id, timeout, deadline) do
    flow_controlled? = not CommandSpec.control_lane?(opcode)

    cond do
      Map.has_key?(state.pending_targets, target) ->
        {:error, :duplicate_request_target, state}

      flow_controlled? and state.data_in_flight >= state.max_in_flight ->
        {:error, :connection_backpressure, state}

      flow_controlled? and
          Map.get(state.pending_lanes, lane_id, 0) >= state.max_in_flight_per_lane ->
        {:error, :connection_backpressure, state}

      true ->
        case ConnectionTimers.remaining(deadline, timeout) do
          0 ->
            {:error, :timeout, state}

          remaining ->
            ConnectionPending.register(
              state,
              target,
              opcode,
              payload,
              lane_id,
              remaining,
              deadline
            )
        end
    end
  end

  @spec complete_encoding(map(), non_neg_integer(), reference(), term()) ::
          {:ok, map()} | {:stop, term(), map()}
  def complete_encoding(state, request_id, encode_token, :ok) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{encode_token: ^encode_token, phase: :sending} = pending} ->
        pending = %{pending | phase: :sent}
        {:ok, %{state | pending: Map.put(state.pending, request_id, pending)}}

      {:ok, %{encode_token: ^encode_token, phase: :discarding}} ->
        {:ok, state}

      _missing_or_stale ->
        {:ok, state}
    end
  end

  def complete_encoding(state, request_id, encode_token, {:transport_error, reason}) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{encode_token: ^encode_token, phase: phase}}
      when phase in [:sending, :discarding] ->
        failure = {:send_failed, reason}
        {:stop, failure, fail_pending(state, failure)}

      _missing_or_stale ->
        {:ok, state}
    end
  end

  def complete_encoding(state, request_id, encode_token, {:error, reason}) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{encode_token: ^encode_token, target: :heartbeat} = pending} ->
        ConnectionTimers.cancel(pending.timer)
        failure = {:heartbeat_failed, reason}
        state = ConnectionPending.drop(state, request_id, pending)
        {:stop, failure, fail_pending(state, {:transport_failed, failure})}

      {:ok, %{encode_token: ^encode_token, phase: :discarding} = pending} ->
        state = ConnectionPending.drop(state, request_id, pending)
        {:ok, ConnectionDrain.maybe_stop(state)}

      {:ok, %{encode_token: ^encode_token} = pending} ->
        ConnectionTimers.cancel(pending.timer)
        ConnectionPending.reply(pending.target, {:error, reason})
        {:ok, ConnectionPending.drop(state, request_id, pending)}

      _missing_or_stale ->
        {:ok, state}
    end
  end

  @spec encoding_ready(map(), non_neg_integer(), reference()) ::
          {:authorize, map()} | {:discard, map()}
  def encoding_ready(state, request_id, encode_token),
    do: encoding_ready(state, request_id, encode_token, nil)

  @spec encoding_ready(map(), non_neg_integer(), reference(), term()) ::
          {:authorize, map()} | {:discard, map()}
  def encoding_ready(state, request_id, encode_token, response_context) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{encode_token: ^encode_token, phase: :encoding} = pending} ->
        pending = %{pending | phase: :sending, response_context: response_context}
        {:authorize, %{state | pending: Map.put(state.pending, request_id, pending)}}

      _missing_or_stale ->
        {:discard, state}
    end
  end

  @spec fail_pending(map(), term()) :: map()
  def fail_pending(state, reason), do: ConnectionPending.fail_all(state, reason)
end
