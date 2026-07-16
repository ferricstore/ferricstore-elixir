defmodule FerricStore.SDK.Native.CoordinatorBatchWireRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{Admission, BatchScheduler, RequestRegistry}
  alias FerricStore.SDK.Native.Coordinator.State

  @spec resume(State.t(), pos_integer(), (State.t(), reference() -> State.t())) :: State.t()
  def resume(state, limit, advance_batch)
      when is_integer(limit) and limit > 0 and is_function(advance_batch, 2) do
    {batch_ids, batch_scheduler} =
      BatchScheduler.take_wire_waiters(state.batch_scheduler, limit)

    state =
      Enum.reduce(batch_ids, %{state | batch_scheduler: batch_scheduler}, fn batch_id, state ->
        resume_batch(state, batch_id, advance_batch)
      end)

    maybe_continue(state)
  end

  @spec resume_capacity(
          {:noreply, State.t()},
          {:ok, term()} | :error,
          pos_integer(),
          (State.t(), reference() -> State.t()),
          (State.t(), term() -> State.t())
        ) :: {:noreply, State.t()}
  def resume_capacity({:noreply, state}, endpoint_key, limit, advance_batch, resume_endpoint) do
    state = resume(state, limit, advance_batch)

    state =
      case endpoint_key do
        {:ok, key} -> resume_endpoint.(state, key)
        :error -> state
      end

    {:noreply, state}
  end

  defp resume_batch(state, batch_id, advance_batch) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :running} -> advance_batch.(state, batch_id)
      _batch_or_nil -> state
    end
  end

  defp maybe_continue(state) do
    available =
      Admission.wire_slots(
        state.limits.pending_requests,
        RequestRegistry.size(state.request_registry)
      )

    if available > 0 and BatchScheduler.wire_waiting_size(state.batch_scheduler) > 0 do
      send(self(), :resume_waiting_batch_wire_slots)
    end

    state
  end
end
