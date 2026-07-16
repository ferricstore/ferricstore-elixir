defmodule FerricStore.SDK.Native.CoordinatorShutdown do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchScheduler,
    CoordinatorReply,
    CoordinatorTimers,
    EventCoordinator,
    EventRestore,
    RefreshOperation,
    RequestRegistry,
    TopologyManager
  }

  @spec run(map(), term()) :: :ok
  def run(state, reason) do
    fail_requests(RequestRegistry.requests(state.request_registry), reason)
    fail_batches(BatchScheduler.batches(state.batch_scheduler), reason)
    fail_event_queue(EventCoordinator.queued_values(state.event_coordinator), reason)
    fail_refresh_waiters(state.topology_manager, reason)
    EventRestore.cancel(EventCoordinator.restore(state.event_coordinator))
    :ok
  end

  defp fail_requests(pending, reason) do
    Enum.each(pending, fn {_tag, request} ->
      CoordinatorTimers.cancel(request.timer)
      CoordinatorTimers.demonitor(Map.get(request, :caller_monitor))

      case request do
        %{kind: :batch_group} -> :ok
        %{from: nil} -> :ok
        %{from: from} -> CoordinatorReply.reply(from, {:error, reason})
      end
    end)
  end

  defp fail_batches(batches, reason) do
    Enum.each(batches, fn {_batch_id, batch} ->
      CoordinatorTimers.cancel(batch.timer)
      CoordinatorTimers.demonitor(batch.caller_monitor)
      CoordinatorTimers.cancel_preparer(batch.preparer)
      GenServer.reply(batch.from, {:error, reason})
    end)
  end

  defp fail_refresh_waiters(manager, reason) do
    operation_waiters =
      case TopologyManager.refresh_operation(manager) do
        nil -> []
        operation -> RefreshOperation.active_waiters(operation)
      end

    manager
    |> TopologyManager.refresh_completion_waiters()
    |> Kernel.++(operation_waiters)
    |> Enum.each(fn
      {:refresh_call, from, monitor, timer, _context} ->
        CoordinatorTimers.cancel(timer)
        CoordinatorTimers.demonitor(monitor)
        GenServer.reply(from, {:error, reason})

      _internal_waiter ->
        :ok
    end)
  end

  defp fail_event_queue(event_calls, reason) do
    Enum.each(event_calls, fn event_call ->
      CoordinatorTimers.cancel(event_call.queue_timer)
      CoordinatorTimers.demonitor(event_call.caller_monitor)

      if event_call.from, do: GenServer.reply(event_call.from, {:error, reason})
    end)
  end
end
