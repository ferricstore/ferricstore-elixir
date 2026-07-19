defmodule FerricStore.SDK.Native.ConnectionResponseDelivery do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionDrain,
    ConnectionPending,
    ConnectionResponseDecoder,
    ConnectionTimers
  }

  @spec begin(map(), non_neg_integer(), map(), pid(), reference()) :: map()
  def begin(state, request_id, pending, worker, decode_token) do
    :ok = ConnectionResponseDecoder.deliver(worker, request_id, decode_token)

    case pending.target do
      {:acknowledged_message, _reply_to, _tag} ->
        await_acknowledgement(state, request_id, pending, decode_token)

      _immediate_delivery ->
        pending = %{pending | phase: :delivering}

        state
        |> ConnectionPending.drop(request_id, pending)
        |> ConnectionDrain.maybe_stop()
    end
  end

  @spec acknowledge(map(), pid(), reference(), reference()) :: map()
  def acknowledge(state, reply_to, tag, delivery_token)
      when is_pid(reply_to) and is_reference(tag) and is_reference(delivery_token) do
    target = {:acknowledged_message, reply_to, tag}

    with {:ok, request_id} <- Map.fetch(state.pending_targets, target),
         {:ok, %{phase: :awaiting_delivery, delivery_token: ^delivery_token} = pending} <-
           Map.fetch(state.pending, request_id) do
      state
      |> ConnectionPending.drop(request_id, pending)
      |> ConnectionDrain.maybe_stop()
    else
      _missing_or_stale -> state
    end
  end

  defp await_acknowledgement(state, request_id, pending, delivery_token) do
    ConnectionTimers.cancel(pending.timer)

    pending =
      Map.merge(pending, %{
        phase: :awaiting_delivery,
        delivery_token: delivery_token,
        timer: nil
      })

    send(self(), :continue_frames)

    %{
      state
      | pending: Map.put(state.pending, request_id, pending),
        decode: clear_decode(state.decode, request_id)
    }
  end

  defp clear_decode({:response, request_id}, request_id), do: nil
  defp clear_decode(decode, _request_id), do: decode
end
