defmodule FerricStore.SDK.Native.CoordinatorBatchPreparationRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.BatchScheduler
  alias FerricStore.SDK.Native.Coordinator.State, as: CoordinatorState

  @spec complete(map(), pid(), reference(), reference(), term(), function()) :: tuple()
  def complete(state, preparer, token, batch_id, result, finish) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :preparing, preparer: %{pid: ^preparer, token: ^token} = operation} = batch ->
        Process.demonitor(operation.monitor, [:flush])

        state =
          CoordinatorState.delete_lifecycle_monitor(
            state,
            operation.monitor,
            {:batch_preparer, batch_id}
          )

        batch = %{batch | preparer: nil}
        scheduler = BatchScheduler.put(state.batch_scheduler, batch)
        finish.(%{state | batch_scheduler: scheduler}, batch, result)

      _batch_or_nil ->
        {:noreply, state}
    end
  end
end
