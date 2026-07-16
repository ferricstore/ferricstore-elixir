defmodule FerricStore.SDK.Native.ConnectionDiscardedResponse do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionDrain,
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
    if retain_credit?(pending),
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

  @spec expire(map(), non_neg_integer(), reference()) ::
          {:noreply, map()} | {:stop, :late_response_timeout, map()}
  def expire(state, request_id, token) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{phase: :discarding, late_response_token: ^token}} ->
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
    chunk_bytes = pending.chunk_bytes
    chunk_frames = pending.chunk_frames

    timer =
      Process.send_after(
        self(),
        {:late_response_timeout, request_id, token},
        grace_timeout(state, pending)
      )

    pending =
      Map.merge(pending, %{
        target: :discard,
        phase: :discarding,
        timer: timer,
        timeout_token: make_ref(),
        late_response_token: token,
        discarded_response_bytes: chunk_bytes,
        discarded_response_frames: chunk_frames,
        chunks: [],
        chunk_bytes: 0,
        chunk_frames: 0
      })

    %{
      state
      | pending: Map.put(state.pending, request_id, pending),
        pending_targets: delete_target(state.pending_targets, target, request_id),
        response_chunk_bytes: max(state.response_chunk_bytes - chunk_bytes, 0),
        response_chunk_frames: max(state.response_chunk_frames - chunk_frames, 0)
    }
  end

  defp retain_credit?(%{flow_controlled?: true, phase: phase})
       when phase in [:sending, :sent],
       do: true

  defp retain_credit?(_pending), do: false

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
