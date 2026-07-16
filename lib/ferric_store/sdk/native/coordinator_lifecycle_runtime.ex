defmodule FerricStore.SDK.Native.CoordinatorLifecycleRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorConnectionRuntime,
    CoordinatorEventRuntime,
    CoordinatorTopologyRefreshRuntime,
    KVPreparationCoordinator,
    LifecycleRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec down(State.t(), reference(), pid(), term(), map()) :: {:noreply, State.t()}
  def down(state, monitor, process, reason, callbacks) do
    {owner, lifecycle_registry} = LifecycleRegistry.pop(state.lifecycle_registry, monitor)
    state = %{state | lifecycle_registry: lifecycle_registry}
    dispatch(owner, state, monitor, process, reason, callbacks)
  end

  defp dispatch({:refresh_waiter, monitor}, state, monitor, _process, _reason, callbacks),
    do: {:noreply, callbacks.abandon_refresh_waiter.(state, monitor)}

  defp dispatch({:pending_request, tag}, state, _monitor, _process, _reason, callbacks),
    do: {:noreply, callbacks.abandon_pending_request.(state, tag)}

  defp dispatch(
         {:preparation_reservation, reservation},
         state,
         _monitor,
         _process,
         _reason,
         _callbacks
       ),
       do: {:noreply, KVPreparationCoordinator.drop(state, reservation)}

  defp dispatch({:batch, batch_id}, state, _monitor, _process, _reason, callbacks),
    do: {:noreply, callbacks.abandon_batch.(state, batch_id)}

  defp dispatch({:batch_preparer, batch_id}, state, _monitor, _process, reason, callbacks),
    do: {:noreply, callbacks.fail_batch_preparer.(state, batch_id, reason)}

  defp dispatch({:event_call, event_call_id}, state, _monitor, _process, _reason, callbacks),
    do: {:noreply, CoordinatorEventRuntime.abandon(state, event_call_id, callbacks.event_runtime)}

  defp dispatch({:connection, process}, state, _monitor, process, reason, callbacks),
    do: callbacks.connection_down.(state, process, reason)

  defp dispatch({:connection_attempt, key}, state, monitor, _process, reason, callbacks) do
    CoordinatorConnectionRuntime.handle_attempt_down(
      state,
      key,
      monitor,
      reason,
      callbacks.connection_started
    )
  end

  defp dispatch({:topology_refresh, token}, state, monitor, _process, reason, callbacks) do
    case CoordinatorTopologyRefreshRuntime.operation(state) do
      %{monitor: ^monitor, token: ^token} = operation ->
        CoordinatorTopologyRefreshRuntime.refresher_down(
          state,
          operation,
          reason,
          callbacks.topology_refresh
        )

      _stale_refresh ->
        {:noreply, state}
    end
  end

  defp dispatch(
         {:event_subscriber, process},
         state,
         monitor,
         process,
         _reason,
         callbacks
       ),
       do: callbacks.subscriber_down.(state, monitor, process)

  defp dispatch(_unindexed, state, _monitor, _process, _reason, _callbacks),
    do: {:noreply, state}
end
