defmodule FerricStore.SDK.Native.TopologyRefreshCompletions do
  @moduledoc false

  alias FerricStore.SDK.Native.Coordinator.State
  alias FerricStore.SDK.Native.{RefreshOperation, TopologyManager}

  @max_resumes_per_tick 64

  @spec enqueue(State.t(), RefreshOperation.t() | [RefreshOperation.waiter()], term()) ::
          State.t()
  def enqueue(%State{} = state, source, result) do
    manager = enqueue_manager(state.topology_manager, source, result)
    send(self(), :resume_topology_refresh_waiters)
    %{state | topology_manager: manager}
  end

  @spec resume(State.t(), (RefreshOperation.waiter(), term(), State.t() -> State.t())) ::
          State.t()
  def resume(%State{} = state, finish_waiter) when is_function(finish_waiter, 3) do
    {completions, manager} =
      TopologyManager.take_refresh_completions(
        state.topology_manager,
        @max_resumes_per_tick
      )

    state =
      Enum.reduce(completions, %{state | topology_manager: manager}, fn {waiter, result}, state ->
        finish_waiter.(waiter, result, state)
      end)

    if not TopologyManager.refresh_completions_empty?(state.topology_manager) do
      send(self(), :resume_topology_refresh_waiters)
    end

    state
  end

  @spec cancel(State.t(), term()) :: State.t()
  def cancel(%State{} = state, key) do
    case TopologyManager.cancel_refresh_completion(state.topology_manager, key) do
      {:ok, manager} -> %{state | topology_manager: manager}
      :missing -> state
    end
  end

  defp enqueue_manager(manager, %RefreshOperation{} = operation, result),
    do: TopologyManager.enqueue_refresh_completion(manager, operation, result)

  defp enqueue_manager(manager, waiters, result) when is_list(waiters),
    do: TopologyManager.enqueue_refresh_waiters(manager, waiters, result)
end
