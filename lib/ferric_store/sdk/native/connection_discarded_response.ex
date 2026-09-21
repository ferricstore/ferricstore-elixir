defmodule FerricStore.SDK.Native.ConnectionDiscardedResponse do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionDrain,
    ConnectionDiscardedControlResponse,
    ConnectionPendingLifecycle,
    ConnectionTimers
  }

  @spec cancel_target(map(), term()) :: map()
  def cancel_target(state, target) do
    case Map.fetch(state.pending_targets, target) do
      {:ok, request_id} ->
        state = %{state | pending_targets: Map.delete(state.pending_targets, target)}

        case Map.fetch(state.pending, request_id) do
          {:ok, %{target: ^target} = pending} -> abandon(state, request_id, pending)
          _missing_or_stale -> state
        end

      :error ->
        state
    end
  end

  @spec abandon(map(), non_neg_integer(), map()) :: map()
  def abandon(state, request_id, pending) do
    if retain_response?(pending),
      do: mark(state, request_id, pending),
      else: ConnectionPendingLifecycle.drop(state, request_id, pending)
  end

  @spec timeout(map(), non_neg_integer(), map()) :: map()
  def timeout(state, request_id, pending) do
    ConnectionPendingLifecycle.reply(pending.target, {:error, :timeout})
    abandon(state, request_id, pending)
  end

  @spec consume(map(), non_neg_integer(), map(), non_neg_integer(), binary()) ::
          {:ok, map()} | {:stop, term(), map()}
  def consume(state, request_id, pending, flags, body) do
    if ConnectionDiscardedControlResponse.authoritative?(pending) do
      ConnectionDiscardedControlResponse.consume(state, request_id, pending, flags, body)
    else
      consume_discarded(state, request_id, pending, flags, body)
    end
  end

  @spec expire(map(), non_neg_integer(), reference()) ::
          {:noreply, map()} | {:stop, :late_response_timeout, map()}
  def expire(state, request_id, token) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{phase: phase, late_response_token: ^token}}
      when phase in [:discarding, :discarding_decoding] ->
        failure = {:transport_failed, :late_response_timeout}
        {:stop, :late_response_timeout, ConnectionPendingLifecycle.fail_all(state, failure)}

      _missing_or_stale ->
        {:noreply, state}
    end
  end

  defp mark(state, request_id, pending) do
    ConnectionTimers.cancel(pending.timer)
    token = make_ref()
    target = pending.target

    {state, pending} = ConnectionDiscardedControlResponse.prepare_buffer(state, pending)

    timer =
      Process.send_after(
        self(),
        {:late_response_timeout, request_id, token},
        grace_timeout(state, pending)
      )

    pending =
      Map.merge(pending, %{
        target: :discard,
        phase: discard_phase(pending.phase),
        timer: timer,
        timeout_token: make_ref(),
        late_response_token: token,
        discarded_response_bytes: Map.get(pending, :discarded_response_bytes, 0),
        discarded_response_frames: Map.get(pending, :discarded_response_frames, 0)
      })

    %{
      state
      | pending: Map.put(state.pending, request_id, pending),
        pending_targets: delete_target(state.pending_targets, target, request_id)
    }
  end

  defp consume_discarded(state, request_id, pending, flags, body) do
    bytes = Map.get(pending, :discarded_response_bytes, 0) + byte_size(body)
    frames = Map.get(pending, :discarded_response_frames, 0) + 1

    cond do
      bytes > state.max_response_bytes ->
        {:stop, :response_too_large, state}

      frames > state.max_response_chunk_frames ->
        {:stop, :response_chunk_frames_too_large, state}

      Codec.more_chunks?(flags) ->
        pending = %{
          pending
          | discarded_response_bytes: bytes,
            discarded_response_frames: frames
        }

        {:ok, %{state | pending: Map.put(state.pending, request_id, pending)}}

      true ->
        state = ConnectionPendingLifecycle.drop(state, request_id, pending)
        {:ok, ConnectionDrain.maybe_stop(state)}
    end
  end

  defp retain_response?(%{phase: phase}) when phase in [:sending, :sent], do: true

  defp retain_response?(%{phase: :decoding} = pending),
    do: ConnectionDiscardedControlResponse.authoritative?(pending)

  defp retain_response?(_pending), do: false

  defp discard_phase(:decoding), do: :discarding_decoding
  defp discard_phase(_phase), do: :discarding

  defp grace_timeout(_state, %{timeout: timeout})
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp grace_timeout(%{drain: %{timeout: timeout}}, _pending)
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp grace_timeout(_state, _pending), do: 5_000

  defp delete_target(pending_targets, target, request_id) do
    case Map.fetch(pending_targets, target) do
      {:ok, ^request_id} -> Map.delete(pending_targets, target)
      _missing_or_newer -> pending_targets
    end
  end
end
