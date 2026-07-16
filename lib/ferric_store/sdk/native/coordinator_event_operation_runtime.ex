defmodule FerricStore.SDK.Native.CoordinatorEventOperationRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorEventConnectionRuntime,
    CoordinatorEventRuntime,
    CoordinatorTimers,
    EventCall,
    EventCommit,
    EventCoordinator,
    EventRequest
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec start(State.t(), EventCall.t(), CoordinatorEventRuntime.callbacks()) ::
          {:noreply, State.t()}
  def start(state, event_call, callbacks) do
    if CoordinatorTimers.expired?(event_call.opts),
      do: finish(state, event_call, {:error, :timeout}, callbacks),
      else: start_planned(state, event_call, callbacks)
  end

  @spec finish(State.t(), EventCall.t(), term(), CoordinatorEventRuntime.callbacks()) ::
          {:noreply, State.t()}
  def finish(state, event_call, result, callbacks) do
    CoordinatorTimers.cancel(event_call.queue_timer)
    CoordinatorTimers.demonitor(event_call.caller_monitor)
    if event_call.from, do: GenServer.reply(event_call.from, result)

    state =
      state
      |> State.put_event_operation(nil)
      |> release_subscriber(event_call)
      |> State.delete_lifecycle_monitor(
        event_call.caller_monitor,
        {:event_call, event_call.id}
      )

    start_next(state, callbacks)
  end

  defp start_planned(state, event_call, callbacks) do
    case {event_call.action, EventCall.plan(event_call, State.event_subscriptions(state))} do
      {_action, {:noop, _changes}} ->
        finish(state, event_call, {:ok, "OK"}, callbacks)

      {:subscribe, {:local, changes}} ->
        state = EventCommit.subscribe(state, event_call.subscriber, changes, nil)
        finish(state, event_call, {:ok, "OK"}, callbacks)

      {:unsubscribe, {:local, changes}} ->
        state = EventCommit.unsubscribe(state, event_call.subscriber, changes)
        finish(state, event_call, {:ok, "OK"}, callbacks)

      {_action, {:wire, changes, wire_events}} ->
        request = EventRequest.operation(event_call, changes, wire_events)
        CoordinatorEventConnectionRuntime.queue_request(state, request, callbacks)
    end
  end

  defp start_next(state, callbacks) do
    case EventCoordinator.out(state.event_coordinator) do
      {{:value, next_call}, coordinator} ->
        CoordinatorTimers.cancel(next_call.queue_timer)
        next_call = EventCall.dequeued(next_call)

        state
        |> Map.put(:event_coordinator, coordinator)
        |> State.put_event_operation(next_call)
        |> start(next_call, callbacks)

      {:empty, coordinator} ->
        {:noreply, %{state | event_coordinator: coordinator}}
    end
  end

  defp release_subscriber(state, %{subscriber_reserved: true, subscriber: subscriber}),
    do: State.release_event_subscriber(state, subscriber)

  defp release_subscriber(state, _event_call), do: state
end
