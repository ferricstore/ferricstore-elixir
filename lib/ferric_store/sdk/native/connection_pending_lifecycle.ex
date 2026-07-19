defmodule FerricStore.SDK.Native.ConnectionPendingLifecycle do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionPendingFailure,
    ConnectionReply,
    ConnectionResponseDecoder,
    ConnectionTimers,
    FlowControl
  }

  @spec drop(map(), non_neg_integer(), map()) :: map()
  def drop(state, request_id, pending) do
    ConnectionTimers.cancel(Map.get(pending, :timer))
    decoding? = pending[:phase] in [:decoding, :delivering]
    ConnectionResponseDecoder.stop(pending)

    state = %{
      state
      | pending: Map.delete(state.pending, request_id),
        pending_targets: delete_target_index(state.pending_targets, pending.target, request_id),
        response_chunk_bytes: max(state.response_chunk_bytes - pending.chunk_bytes, 0),
        response_chunk_frames: max(state.response_chunk_frames - pending.chunk_frames, 0)
    }

    state = FlowControl.decrement(state, pending)

    if decoding? do
      send(self(), :continue_frames)
      clear_response_decode(state, request_id)
    else
      state
    end
  end

  @spec fail_all(map(), term()) :: map()
  def fail_all(state, reason) do
    state |> ConnectionPendingFailure.run(reason) |> clear_response_decode()
  end

  @spec discard_all(map()) :: map()
  def discard_all(state) do
    Enum.each(state.pending, fn {_request_id, pending} ->
      ConnectionTimers.cancel(pending.timer)
      ConnectionResponseDecoder.stop(pending)
    end)

    state
    |> Map.merge(%{
      pending: %{},
      pending_targets: %{},
      pending_lanes: %{},
      data_in_flight: 0,
      response_chunk_bytes: 0,
      response_chunk_frames: 0
    })
    |> clear_response_decode()
  end

  @spec awaiting_delivery?(map()) :: boolean()
  def awaiting_delivery?(state),
    do: Enum.any?(state.pending, fn {_request_id, pending} -> delivery_pending?(pending) end)

  @spec reply(term(), term()) :: :ok | term()
  defdelegate reply(target, result), to: ConnectionReply, as: :send

  defp delivery_pending?(pending), do: pending[:phase] == :awaiting_delivery

  defp delete_target_index(pending_targets, target, request_id) do
    case Map.fetch(pending_targets, target) do
      {:ok, ^request_id} -> Map.delete(pending_targets, target)
      _missing_or_newer -> pending_targets
    end
  end

  defp clear_response_decode(state, request_id) do
    if Map.get(state, :decode) == {:response, request_id},
      do: Map.put(state, :decode, nil),
      else: state
  end

  defp clear_response_decode(state) do
    case Map.get(state, :decode) do
      {:response, _request_id} -> Map.put(state, :decode, nil)
      _server_or_idle -> state
    end
  end
end
