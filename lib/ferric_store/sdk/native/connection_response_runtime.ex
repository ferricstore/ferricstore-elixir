defmodule FerricStore.SDK.Native.ConnectionResponseRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionDrain,
    ConnectionEventHandler,
    ConnectionPending,
    ConnectionRequest,
    ConnectionResponseDecoder,
    ConnectionTimers,
    FlowControl
  }

  @spec finish(map(), non_neg_integer(), map(), non_neg_integer(), binary()) ::
          {:ok, map()} | {:stop, term(), map()}
  def finish(state, request_id, pending, flags, body) do
    if ConnectionTimers.expired?(pending.deadline) do
      complete(state, request_id, pending, {:error, :timeout})
    else
      begin_decode(state, request_id, pending, flags, body)
    end
  end

  @spec complete_decode(map(), pid(), non_neg_integer(), reference(), term()) ::
          {:ok, map()} | {:stop, term(), map()}
  def complete_decode(state, worker, request_id, decode_token, result) do
    case Map.fetch(state.pending, request_id) do
      {:ok,
       %{
         phase: :decoding,
         decode_worker: ^worker,
         decode_token: ^decode_token
       } = pending} ->
        if ConnectionTimers.expired?(pending.deadline) do
          complete(state, request_id, pending, {:error, :timeout})
        else
          accept_decode(state, request_id, pending, worker, decode_token, result)
        end

      _missing_or_stale ->
        {:ok, state}
    end
  end

  defp begin_decode(state, request_id, pending, flags, body) do
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
          max_response_bytes: state.max_response_bytes,
          response_context: pending.response_context
        }
      )

    decoding =
      Map.merge(pending, %{
        phase: :decoding,
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
      |> apply_window_update(pending.opcode, result)
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
    previous = capacity_profile(state)
    state = FlowControl.apply_window_limits(state, window_limits)
    notify_capacity_change(state, previous)
    :ok = ConnectionResponseDecoder.deliver(worker, request_id, decode_token)
    delivering = %{pending | phase: :delivering}
    state = ConnectionPending.drop(state, request_id, delivering)
    ConnectionTimers.cancel(pending.timer)
    {:ok, ConnectionDrain.maybe_stop(state)}
  end

  defp accept_decode(state, _request_id, _pending, _worker, _decode_token, _metadata) do
    failure = :invalid_response_decode_metadata
    {:stop, failure, ConnectionRequest.fail_pending(state, failure)}
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
    reply_target(pending.target, result)
    {:ok, ConnectionDrain.maybe_stop(state)}
  end

  defp apply_window_update(state, opcode, result) do
    previous = capacity_profile(state)
    next_state = FlowControl.apply_window_update(state, opcode, result)
    notify_capacity_change(next_state, previous)

    next_state
  end

  defp notify_capacity_change(state, previous) do
    capacity = capacity_profile(state)

    if capacity != previous do
      ConnectionEventHandler.capacity_changed(state.event_handler, self(), capacity)
    end
  end

  defp capacity_profile(state) do
    %{
      max_in_flight: state.max_in_flight,
      max_in_flight_per_lane: state.max_in_flight_per_lane
    }
  end

  defp reply_target({:call, from}, result), do: GenServer.reply(from, result)

  defp reply_target({:message, reply_to, tag}, result),
    do: send(reply_to, {:ferricstore_connection_response, self(), tag, result})
end
