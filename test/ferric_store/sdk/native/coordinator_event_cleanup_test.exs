defmodule FerricStore.SDK.Native.CoordinatorEventCleanupTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{
    CoordinatorEventRuntime,
    CoordinatorTimers,
    EventCall,
    EventCoordinator,
    EventSubscriptions
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "queued dead-subscriber cleanup cannot expire and leave stale ownership" do
    subscriber = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(subscriber), do: Process.exit(subscriber, :kill) end)
    events = EventSubscriptions.normalize([:flow_wake])

    subscriptions =
      EventSubscriptions.new()
      |> EventSubscriptions.subscribe(subscriber, events, self())

    cleanup = EventCall.subscriber_down(subscriber, 500)
    assert CoordinatorTimers.event_queue_timer(cleanup) == nil

    coordinator =
      %EventCoordinator{subscriptions: subscriptions}
      |> EventCoordinator.enqueue(cleanup)

    state = %State{event_coordinator: coordinator}

    assert {:noreply, ^state} = CoordinatorEventRuntime.timeout_queued(state, cleanup.id)
    assert EventCoordinator.queue_size(state.event_coordinator) == 1

    assert EventSubscriptions.subscriber(
             EventCoordinator.subscriptions(state.event_coordinator),
             subscriber
           )
  end
end
