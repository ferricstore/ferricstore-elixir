defmodule FerricStore.SDK.Native.TopologyRefreshWaiter do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchCoordinator,
    BatchScheduler,
    CoordinatorBatchRuntime,
    CoordinatorTimers
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec finish(term(), term(), State.t(), map(), function()) :: State.t()
  def finish({:refresh_call, from, monitor, timer, context}, result, state, _callbacks, _restart) do
    CoordinatorTimers.cancel(timer)
    Process.demonitor(monitor, [:flush])

    result = if CoordinatorTimers.expired?(context), do: {:error, :timeout}, else: result
    GenServer.reply(from, result)

    state
    |> State.delete_lifecycle_monitor(monitor, {:refresh_waiter, monitor})
    |> State.adjust_refresh_calls(-1)
  end

  def finish({:request_retry, tag}, :ok, state, callbacks, _restart),
    do: callbacks.dispatch_request_retry.(state, tag)

  def finish({:request_retry, tag}, {:error, reason}, state, callbacks, _restart),
    do: callbacks.fail_request_retry.(state, tag, reason)

  def finish({:batch_retry, batch_id}, :ok, state, _callbacks, _restart) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :refreshing} = batch ->
        case BatchCoordinator.begin_preparation(state, batch) do
          {:ok, state} -> state
          {:error, reason, state} -> CoordinatorBatchRuntime.fail_retry(state, batch_id, reason)
        end

      _batch_or_nil ->
        state
    end
  end

  def finish({:batch_retry, batch_id}, {:error, reason}, state, _callbacks, _restart),
    do: CoordinatorBatchRuntime.fail_retry(state, batch_id, reason)

  def finish(:topology_event, _result, state, _callbacks, _restart), do: state

  def finish(:topology_event_followup, _result, state, callbacks, restart) do
    {:noreply, state} = restart.(state, :topology_event, callbacks)
    state
  end
end
