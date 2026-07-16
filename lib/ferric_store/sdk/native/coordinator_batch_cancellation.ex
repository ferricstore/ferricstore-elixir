defmodule FerricStore.SDK.Native.CoordinatorBatchCancellation do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchExecution,
    BatchScheduler,
    ConnectionLifecycle,
    ConnectionPool,
    CoordinatorTimers
  }

  alias FerricStore.SDK.Native.Coordinator.State

  def cancel(state, batch_id, reply) do
    case BatchScheduler.pop(state.batch_scheduler, batch_id) do
      {nil, _batch_scheduler} ->
        {:ok, state}

      {batch, batch_scheduler} ->
        CoordinatorTimers.cancel(batch.timer)
        CoordinatorTimers.demonitor(batch.caller_monitor)
        CoordinatorTimers.cancel_preparer(batch.preparer)
        if reply, do: GenServer.reply(batch.from, reply)

        state =
          state
          |> Map.put(:batch_scheduler, batch_scheduler)
          |> BatchExecution.release_preflight(batch)
          |> BatchExecution.cancel_requests(batch)
          |> clear_lifecycle(batch)

        {state, resume?} = remove_connection_waiters(state, batch_id)
        {:cleanup, state, {:batch_retry, batch_id}, resume?}
    end
  end

  defp remove_connection_waiters(state, batch_id) do
    {emptied, pool} = ConnectionPool.remove_batch_waiters(state.connection_pool, batch_id)
    ConnectionLifecycle.stop_attempts(state.operation_supervisor, emptied)

    state =
      Enum.reduce(emptied, %{state | connection_pool: pool}, fn {key, attempt}, state ->
        State.delete_lifecycle_monitor(state, attempt.monitor, {:connection_attempt, key})
      end)

    {state, emptied != []}
  end

  defp clear_lifecycle(state, batch) do
    state = State.delete_lifecycle_monitor(state, batch.caller_monitor, {:batch, batch.id})

    case batch.preparer do
      %{monitor: monitor} ->
        CoordinatorTimers.demonitor(monitor)
        State.delete_lifecycle_monitor(state, monitor, {:batch_preparer, batch.id})

      nil ->
        state
    end
  end
end
