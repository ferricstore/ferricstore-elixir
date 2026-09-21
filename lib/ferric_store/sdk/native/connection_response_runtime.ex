defmodule FerricStore.SDK.Native.ConnectionResponseRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionDrain,
    ConnectionDiscardedControlResponse,
    ConnectionDiscardedResponse,
    ConnectionPending,
    ConnectionRequest,
    ConnectionResponseCapacity,
    ConnectionResponseDecoder,
    ConnectionResponseDelivery,
    ConnectionTimers
  }

  @spec finish(map(), non_neg_integer(), map(), non_neg_integer(), iodata(), non_neg_integer()) ::
          {:ok, map()} | {:stop, term(), map()}
  def finish(state, request_id, pending, flags, body, body_bytes) do
    if ConnectionTimers.expired?(pending.deadline) and
         ConnectionDiscardedControlResponse.authoritative?(pending) do
      state = ConnectionDiscardedResponse.timeout(state, request_id, pending)

      begin_discarded_decode(
        state,
        request_id,
        state.pending[request_id],
        flags,
        body,
        body_bytes
      )
    else
      if ConnectionTimers.expired?(pending.deadline) do
        complete(state, request_id, pending, {:error, :timeout})
      else
        begin_decode(state, request_id, pending, flags, body, body_bytes)
      end
    end
  end

  @spec complete_decode(map(), pid(), non_neg_integer(), reference(), term()) ::
          {:ok, map()} | {:stop, term(), map()}
  def complete_decode(state, worker, request_id, decode_token, result) do
    case Map.fetch(state.pending, request_id) do
      {:ok,
       %{
         phase: :discarding_decoding,
         decode_worker: ^worker,
         decode_token: ^decode_token
       } = pending} ->
        accept_discarded_decode(state, request_id, pending, result)

      {:ok,
       %{
         phase: :decoding,
         decode_worker: ^worker,
         decode_token: ^decode_token
       } = pending} ->
        if ConnectionTimers.expired?(pending.deadline) do
          if ConnectionDiscardedControlResponse.authoritative?(pending) do
            state = ConnectionDiscardedResponse.timeout(state, request_id, pending)
            accept_discarded_decode(state, request_id, state.pending[request_id], result)
          else
            complete(state, request_id, pending, {:error, :timeout})
          end
        else
          accept_decode(state, request_id, pending, worker, decode_token, result)
        end

      _missing_or_stale ->
        {:ok, state}
    end
  end

  def begin_discarded_decode(state, request_id, pending, flags, body, body_bytes),
    do: begin_decode(state, request_id, pending, flags, body, body_bytes, :discarding_decoding)

  defp begin_decode(state, request_id, pending, flags, body, body_bytes),
    do: begin_decode(state, request_id, pending, flags, body, body_bytes, :decoding)

  defp begin_decode(state, request_id, pending, flags, body, body_bytes, phase) do
    decode_token = make_ref()

    worker =
      ConnectionResponseDecoder.start(
        self(),
        request_id,
        decode_token,
        %{
          target: pending.target,
          opcode: pending.opcode,
          flags: flags,
          body: body,
          body_bytes: body_bytes,
          max_response_bytes: state.max_response_bytes,
          response_context: pending.response_context,
          decode_gate: Map.get(pending, :decode_gate)
        }
      )

    decoding =
      Map.merge(pending, %{
        phase: phase,
        decode_token: decode_token,
        decode_worker: worker,
        chunks: [],
        chunk_bytes: 0,
        chunk_frames: 0
      })

    {:ok,
     %{
       state
       | pending: Map.put(state.pending, request_id, decoding),
         response_chunk_bytes: max(state.response_chunk_bytes - pending.chunk_bytes, 0),
         response_chunk_frames: max(state.response_chunk_frames - pending.chunk_frames, 0)
     }
     |> Map.put(:decode, {:response, request_id})}
  end

  defp complete(state, request_id, pending, result) do
    state =
      state
      |> ConnectionResponseCapacity.apply_window_update(pending.opcode, result)
      |> ConnectionPending.drop(request_id, pending)

    complete_target(state, pending, result)
  end

  defp accept_decode(
         state,
         request_id,
         %{target: :heartbeat} = pending,
         _worker,
         _decode_token,
         {:heartbeat, :ok}
       ) do
    complete(state, request_id, pending, {:ok, nil})
  end

  defp accept_decode(
         state,
         request_id,
         %{target: :heartbeat} = pending,
         _worker,
         _decode_token,
         {:heartbeat, {:error, reason}}
       ) do
    complete(state, request_id, pending, {:error, reason})
  end

  defp accept_decode(
         state,
         request_id,
         %{target: target} = pending,
         worker,
         decode_token,
         {:response, window_limits}
       )
       when target != :heartbeat do
    state = ConnectionResponseCapacity.apply_window_limits(state, window_limits)
    {:ok, ConnectionResponseDelivery.begin(state, request_id, pending, worker, decode_token)}
  end

  defp accept_decode(state, _request_id, _pending, _worker, _decode_token, _metadata) do
    failure = :invalid_response_decode_metadata
    {:stop, failure, ConnectionRequest.fail_pending(state, failure)}
  end

  defp accept_discarded_decode(state, request_id, pending, {:response, window_limits}) do
    state = ConnectionResponseCapacity.apply_window_limits(state, window_limits)
    state = ConnectionPending.drop(state, request_id, pending)
    {:ok, ConnectionDrain.maybe_stop(state)}
  end

  defp accept_discarded_decode(state, request_id, pending, _metadata) do
    state = ConnectionPending.drop(state, request_id, pending)
    {:ok, ConnectionDrain.maybe_stop(state)}
  end

  defp complete_target(state, %{target: :heartbeat} = pending, {:ok, _value}) do
    ConnectionTimers.cancel(pending.timer)
    {:ok, ConnectionTimers.schedule_heartbeat(state)}
  end

  defp complete_target(state, %{target: :heartbeat} = pending, {:error, reason}) do
    ConnectionTimers.cancel(pending.timer)
    failure = {:heartbeat_failed, reason}
    {:stop, failure, ConnectionRequest.fail_pending(state, {:transport_failed, failure})}
  end

  defp complete_target(state, pending, result) do
    ConnectionTimers.cancel(pending.timer)
    ConnectionPending.reply(pending.target, result)
    {:ok, ConnectionDrain.maybe_stop(state)}
  end
end
