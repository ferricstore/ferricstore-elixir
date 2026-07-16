defmodule FerricStore.SDK.Native.EventSubscriptionAdmissionTest do
  use ExUnit.Case, async: false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    CoordinatorEventRuntime,
    EventCoordinator,
    EventSubscriptionCoordinator,
    EventSubscriptions
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "pending subscriptions reserve distinct subscriber capacity" do
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(first), do: Process.exit(first, :kill)
      if Process.alive?(second), do: Process.exit(second, :kill)
    end)

    context = RequestContext.new([], 1_000)
    state = put_in(%State{}.limits.event_subscribers, 1)

    assert {:ok, _call, state} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               first,
               [:flow_wake],
               context
             )

    assert {:error, :event_subscriber_backpressure} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               second,
               [:flow_wake],
               context
             )
  end

  test "repeated pending subscriptions for one subscriber share capacity" do
    subscriber = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(subscriber), do: Process.exit(subscriber, :kill) end)

    context = RequestContext.new([], 1_000)
    state = put_in(%State{}.limits.event_subscribers, 1)

    assert {:ok, first_call, state} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               subscriber,
               [:flow_wake],
               context
             )

    assert {:ok, second_call, _state} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               subscriber,
               [:topology_changed],
               context
             )

    assert first_call.subscriber == subscriber
    assert second_call.subscriber == subscriber
    assert EventSubscriptions.subscriber_count(State.event_subscriptions(state)) == 0
  end

  test "timing out a queued subscription releases its subscriber reservation" do
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(first), do: Process.exit(first, :kill)
      if Process.alive?(second), do: Process.exit(second, :kill)
    end)

    context = RequestContext.new([], 1_000)
    state = put_in(%State{}.limits.event_subscribers, 1)

    assert {:ok, call, state} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               first,
               [:flow_wake],
               context
             )

    state = State.put_event_operation(state, %{id: make_ref()})
    assert {:noreply, state} = CoordinatorEventRuntime.enqueue(state, call, %{})
    assert EventCoordinator.subscriber_reservation_count(state.event_coordinator) == 1

    assert {:noreply, state} = CoordinatorEventRuntime.timeout_queued(state, call.id)
    assert EventCoordinator.subscriber_reservation_count(state.event_coordinator) == 0

    assert {:ok, _call, _state} =
             EventSubscriptionCoordinator.prepare(
               state,
               {self(), make_ref()},
               :subscribe,
               second,
               [:flow_wake],
               context
             )
  end

  test "subscriber admission stays constant-time as pending reservations grow" do
    coordinator =
      Enum.reduce(10_000..14_999, %EventCoordinator{}, fn id, coordinator ->
        subscriber = :erlang.list_to_pid(~c"<0.#{id}.0>")

        assert {:ok, coordinator} =
                 EventCoordinator.reserve_subscriber(coordinator, subscriber, 5_001)

        coordinator
      end)

    subscriber = :erlang.list_to_pid(~c"<0.15000.0>")
    :erlang.garbage_collect(self())
    {:reductions, before_reserve} = Process.info(self(), :reductions)
    result = EventCoordinator.reserve_subscriber(coordinator, subscriber, 5_001)
    {:reductions, after_reserve} = Process.info(self(), :reductions)

    assert {:ok, _coordinator} = result
    assert after_reserve - before_reserve < 2_000
  end

  test "reservation count remains exact across commit and unsubscribe transitions" do
    peer = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(peer), do: Process.exit(peer, :kill) end)
    events = EventSubscriptions.normalize([:flow_wake])

    assert {:ok, coordinator} =
             EventCoordinator.reserve_subscriber(%EventCoordinator{}, self(), 1)

    assert {:ok, coordinator} = EventCoordinator.reserve_subscriber(coordinator, self(), 1)
    coordinator = EventCoordinator.subscribe(coordinator, self(), events, nil)
    coordinator = EventCoordinator.release_subscriber(coordinator, self())
    coordinator = EventCoordinator.unsubscribe(coordinator, self(), events)

    assert :full = EventCoordinator.reserve_subscriber(coordinator, peer, 1)

    coordinator = EventCoordinator.release_subscriber(coordinator, self())
    assert {:ok, _coordinator} = EventCoordinator.reserve_subscriber(coordinator, peer, 1)
  end
end
