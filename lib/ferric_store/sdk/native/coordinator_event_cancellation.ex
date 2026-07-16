defmodule FerricStore.SDK.Native.CoordinatorEventCancellation do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Connection,
    CoordinatorEventConnectionRuntime,
    CoordinatorEventOperationRuntime,
    CoordinatorEventRuntime,
    CoordinatorTimers,
    EventCoordinator
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec abandon(State.t(), reference(), CoordinatorEventRuntime.callbacks()) :: State.t()
  def abandon(%State{} = state, event_call_id, callbacks) when is_reference(event_call_id) do
    case State.event_operation(state) do
      %{id: ^event_call_id} = event_call ->
        state = cancel_pending_request(state, event_call_id, callbacks)

        {:noreply, state} =
          CoordinatorEventOperationRuntime.finish(
            state,
            event_call,
            {:error, :caller_down},
            callbacks
          )

        state

      _other ->
        abandon_queued(state, event_call_id)
    end
  end

  defp abandon_queued(state, event_call_id) do
    case EventCoordinator.pop(state.event_coordinator, event_call_id) do
      {nil, _coordinator} ->
        state

      {event_call, coordinator} ->
        CoordinatorTimers.cancel(event_call.queue_timer)
        CoordinatorTimers.demonitor(event_call.caller_monitor)

        state
        |> Map.put(:event_coordinator, coordinator)
        |> release_subscriber(event_call)
        |> State.delete_lifecycle_monitor(
          event_call.caller_monitor,
          {:event_call, event_call.id}
        )
    end
  end

  defp cancel_pending_request(state, event_call_id, callbacks) do
    case State.event_operation(state) do
      %{id: ^event_call_id, request_tag: tag} when is_reference(tag) ->
        cancel_pending_request_tag(state, tag, callbacks)

      _no_pending_request ->
        state
    end
  end

  defp cancel_pending_request_tag(state, tag, callbacks) do
    case State.pop_pending_request(state, tag) do
      {nil, state} ->
        state

      {request, state} ->
        CoordinatorTimers.cancel(request.timer)

        if is_pid(Map.get(request, :conn)) do
          Connection.cancel_async(request.conn, self(), tag)
        end

        state
        |> callbacks.remove_connection_waiter.(Map.get(request, :connection_key), tag)
        |> CoordinatorEventConnectionRuntime.reset(request, :event_caller_down, callbacks)
        |> callbacks.resume_waiting_wire_slots.()
    end
  end

  defp release_subscriber(state, %{subscriber_reserved: true, subscriber: subscriber}),
    do: State.release_event_subscriber(state, subscriber)

  defp release_subscriber(state, _event_call), do: state
end
