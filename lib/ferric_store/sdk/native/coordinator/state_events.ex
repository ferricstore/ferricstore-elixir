defmodule FerricStore.SDK.Native.Coordinator.StateEvents do
  @moduledoc false

  alias FerricStore.SDK.Native.EventCoordinator

  def subscriptions(%{event_coordinator: coordinator}),
    do: EventCoordinator.subscriptions(coordinator)

  def restore(%{event_coordinator: coordinator}), do: EventCoordinator.restore(coordinator)

  def put_restore(state, restore),
    do: put_coordinator(state, EventCoordinator.put_restore(state.event_coordinator, restore))

  def operation(%{event_coordinator: coordinator}), do: EventCoordinator.operation(coordinator)

  def put_operation(state, operation),
    do: put_coordinator(state, EventCoordinator.put_operation(state.event_coordinator, operation))

  def clear_connection(state, connection),
    do:
      put_coordinator(
        state,
        EventCoordinator.clear_connection(state.event_coordinator, connection)
      )

  def put_connection(state, connection),
    do:
      put_coordinator(state, EventCoordinator.put_connection(state.event_coordinator, connection))

  def connection(%{event_coordinator: coordinator}), do: EventCoordinator.connection(coordinator)

  def live_connection?(%{event_coordinator: coordinator}),
    do: EventCoordinator.live_connection?(coordinator)

  def subscriptions_empty?(%{event_coordinator: coordinator}),
    do: EventCoordinator.subscriptions_empty?(coordinator)

  def reserve_subscriber(state, subscriber, limit) do
    case EventCoordinator.reserve_subscriber(state.event_coordinator, subscriber, limit) do
      {:ok, coordinator} -> {:ok, put_coordinator(state, coordinator)}
      :full -> :full
    end
  end

  def release_subscriber(state, subscriber),
    do:
      put_coordinator(
        state,
        EventCoordinator.release_subscriber(state.event_coordinator, subscriber)
      )

  def subscribe(state, subscriber, events, connection),
    do:
      put_coordinator(
        state,
        EventCoordinator.subscribe(state.event_coordinator, subscriber, events, connection)
      )

  def unsubscribe(state, subscriber, events),
    do:
      put_coordinator(
        state,
        EventCoordinator.unsubscribe(state.event_coordinator, subscriber, events)
      )

  defp put_coordinator(state, coordinator), do: %{state | event_coordinator: coordinator}
end
