defmodule FerricStore.SDK.Native.CoordinatorEventQueueRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorEventOperationRuntime,
    CoordinatorEventRuntime,
    CoordinatorTimers,
    EventCall,
    EventCoordinator
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec enqueue(State.t(), EventCall.t(), CoordinatorEventRuntime.callbacks()) ::
          {:noreply, State.t()}
  def enqueue(%State{} = state, %EventCall{} = event_call, callbacks) do
    case State.event_operation(state) do
      nil ->
        state
        |> State.put_event_operation(event_call)
        |> CoordinatorEventOperationRuntime.start(event_call, callbacks)

      _active_operation ->
        event_call = EventCall.queued(event_call, CoordinatorTimers.event_queue_timer(event_call))
        coordinator = EventCoordinator.enqueue(state.event_coordinator, event_call)
        {:noreply, %{state | event_coordinator: coordinator}}
    end
  end

  @spec timeout(State.t(), reference()) :: {:noreply, State.t()}
  def timeout(%State{} = state, event_call_id) when is_reference(event_call_id) do
    case EventCoordinator.fetch(state.event_coordinator, event_call_id) do
      %EventCall{subscriber_down: true} -> {:noreply, state}
      _public_or_missing -> timeout_public_call(state, event_call_id)
    end
  end

  defp timeout_public_call(state, event_call_id) do
    case EventCoordinator.pop(state.event_coordinator, event_call_id) do
      {nil, _coordinator} ->
        {:noreply, state}

      {event_call, coordinator} ->
        CoordinatorTimers.demonitor(event_call.caller_monitor)

        state =
          state
          |> Map.put(:event_coordinator, coordinator)
          |> release_subscriber(event_call)
          |> State.delete_lifecycle_monitor(
            event_call.caller_monitor,
            {:event_call, event_call.id}
          )

        if event_call.from, do: GenServer.reply(event_call.from, {:error, :timeout})
        {:noreply, state}
    end
  end

  defp release_subscriber(state, %{subscriber_reserved: true, subscriber: subscriber}),
    do: State.release_event_subscriber(state, subscriber)

  defp release_subscriber(state, _event_call), do: state
end
