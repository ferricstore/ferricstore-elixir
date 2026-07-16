defmodule FerricStore.SDK.Native.CoordinatorBatchCompletion do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchExecution,
    BatchRetry,
    BatchScheduler,
    CoordinatorTimers
  }

  alias FerricStore.SDK.Native.Coordinator.State

  def fail_preparer(state, batch_id, reason) do
    case BatchScheduler.pop(state.batch_scheduler, batch_id) do
      {nil, _batch_scheduler} ->
        {:ok, state}

      {batch, batch_scheduler} ->
        batch = %{batch | preparer: nil}
        state = %{state | batch_scheduler: batch_scheduler}
        {:ok, reply_error(state, batch, {:batch_preparer_failed, reason})}
    end
  end

  def fail_retry(state, batch_id, reason) do
    case BatchScheduler.pop(state.batch_scheduler, batch_id) do
      {nil, _batch_scheduler} ->
        state

      {batch, batch_scheduler} ->
        reply_error(%{state | batch_scheduler: batch_scheduler}, batch, reason)
    end
  end

  def finish(state, batch_id) do
    {completion, state, batch} = BatchExecution.take_completion(state, batch_id)

    case completion do
      {:ok, successes} ->
        CoordinatorTimers.cancel(batch.timer)
        GenServer.reply(batch.from, {:ok, successes})
        {:ok, clear_lifecycle(state, batch)}

      {:retry, original_reason} ->
        retry(state, batch, original_reason)

      {:error, reason} ->
        {:ok, reply_error(state, batch, reason)}
    end
  end

  def reply_error(state, batch, reason) do
    reason = retry_reason(batch, reason)
    CoordinatorTimers.cancel(batch.timer)
    CoordinatorTimers.cancel_preparer(batch.preparer)
    GenServer.reply(batch.from, {:error, reason})
    clear_lifecycle(state, batch)
  end

  defp retry(state, batch, original_reason) do
    {:ok, batch} = BatchRetry.prepare(batch, original_reason)
    scheduler = BatchScheduler.put(state.batch_scheduler, batch)
    {:refresh, %{state | batch_scheduler: scheduler}, {:batch_retry, batch.id}}
  end

  defp retry_reason(batch, reason) do
    if batch.attempt > 0 and not match?({:retry_failed, _, _}, reason),
      do: {:retry_failed, batch.original_reason, reason},
      else: reason
  end

  defp clear_lifecycle(state, batch) do
    CoordinatorTimers.demonitor(batch.caller_monitor)
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
