defmodule FerricStore.SDK.Native.EventSubscriptionsTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.EventSubscriptions

  test "subscription commits own subscriber refcounts and wire deltas" do
    subscriptions = EventSubscriptions.new()
    requested = EventSubscriptions.normalize([:flow_wake, "TOPOLOGY_CHANGED"])

    assert EventSubscriptions.subscribe_wire_events(subscriptions, requested) ==
             MapSet.new(["FLOW_WAKE"])

    subscriptions = EventSubscriptions.subscribe(subscriptions, self(), requested, self())

    assert EventSubscriptions.refcounts(subscriptions) == %{
             "FLOW_WAKE" => 1,
             "TOPOLOGY_CHANGED" => 1
           }

    assert EventSubscriptions.subscriber_events(subscriptions, self()) == requested
    assert EventSubscriptions.connection(subscriptions) == self()
    assert EventSubscriptions.subscribe_wire_events(subscriptions, requested) == MapSet.new()

    assert EventSubscriptions.unsubscribe_wire_events(subscriptions, requested) ==
             MapSet.new(["FLOW_WAKE"])

    subscriptions = EventSubscriptions.unsubscribe(subscriptions, self(), requested)
    refute EventSubscriptions.empty?(subscriptions)
    assert EventSubscriptions.desired_events(subscriptions) == MapSet.new(["TOPOLOGY_CHANGED"])
    assert EventSubscriptions.connection(subscriptions) == self()
  end

  test "all-events subscriptions collapse the supported wire payload" do
    requested = EventSubscriptions.normalize([])
    assert requested == MapSet.new([{:ferricstore, :all_events}])

    assert EventSubscriptions.wire_payload(requested) == [
             "AUTH_INVALIDATED",
             "BACKPRESSURE_CHANGED",
             "FLOW_WAKE",
             "GOAWAY",
             "TOPOLOGY_CHANGED"
           ]
  end

  test "duplicate subscription and unsubscription transitions are idempotent" do
    requested = EventSubscriptions.normalize([:flow_wake])

    subscriptions =
      EventSubscriptions.new()
      |> EventSubscriptions.subscribe(self(), requested, self())
      |> EventSubscriptions.subscribe(self(), requested, self())

    assert EventSubscriptions.refcounts(subscriptions) == %{"FLOW_WAKE" => 1}

    subscriptions =
      subscriptions
      |> EventSubscriptions.unsubscribe(self(), requested)
      |> EventSubscriptions.unsubscribe(self(), requested)

    refute EventSubscriptions.empty?(subscriptions)
    assert EventSubscriptions.desired_events(subscriptions) == MapSet.new(["TOPOLOGY_CHANGED"])
    assert EventSubscriptions.subscriber(subscriptions, self()) == nil
  end

  test "one subscriber cannot consume another subscriber's refcount" do
    other = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(other), do: Process.exit(other, :kill) end)
    requested = EventSubscriptions.normalize([:flow_wake])

    subscriptions =
      EventSubscriptions.new()
      |> EventSubscriptions.subscribe(self(), requested, self())
      |> EventSubscriptions.subscribe(other, requested, self())
      |> EventSubscriptions.unsubscribe(self(), requested)
      |> EventSubscriptions.unsubscribe(self(), requested)

    assert EventSubscriptions.refcounts(subscriptions) == %{"FLOW_WAKE" => 1}
    assert EventSubscriptions.subscriber_events(subscriptions, other) == requested
  end

  test "removing all-events preserves specific events owned by another subscriber" do
    other = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(other), do: Process.exit(other, :kill) end)

    all_events = EventSubscriptions.normalize([])
    flow_wake = EventSubscriptions.normalize([:flow_wake])

    subscriptions =
      EventSubscriptions.new()
      |> EventSubscriptions.subscribe(self(), all_events, self())
      |> EventSubscriptions.subscribe(other, flow_wake, self())

    assert %{changes: ^all_events, wire_events: wire_events} =
             EventSubscriptions.plan_unsubscribe(subscriptions, self(), [])

    refute "FLOW_WAKE" in EventSubscriptions.wire_payload(wire_events)

    subscriptions = EventSubscriptions.unsubscribe(subscriptions, self(), all_events)
    assert EventSubscriptions.refcounts(subscriptions) == %{"FLOW_WAKE" => 1}
  end

  test "removing all filters also removes a specific filter owned by the same subscriber" do
    all_events = EventSubscriptions.normalize([])
    flow_wake = EventSubscriptions.normalize([:flow_wake])

    subscriptions =
      EventSubscriptions.new()
      |> EventSubscriptions.subscribe(self(), all_events, self())
      |> EventSubscriptions.subscribe(self(), flow_wake, self())

    assert %{changes: changes, wire_events: wire_events} =
             EventSubscriptions.plan_unsubscribe(subscriptions, self(), [])

    assert changes == MapSet.union(all_events, flow_wake)
    assert "FLOW_WAKE" in EventSubscriptions.wire_payload(wire_events)
    refute "TOPOLOGY_CHANGED" in EventSubscriptions.wire_payload(wire_events)
  end

  test "subscription plans own local and wire deltas" do
    requested = [:flow_wake, :topology_changed]
    subscriptions = EventSubscriptions.new()

    assert %{changes: changes, wire_events: wire_events} =
             EventSubscriptions.plan_subscribe(subscriptions, self(), requested)

    assert changes == EventSubscriptions.normalize(requested)
    assert wire_events == MapSet.new(["FLOW_WAKE"])

    subscriptions = EventSubscriptions.subscribe(subscriptions, self(), changes, self())

    assert EventSubscriptions.plan_subscribe(subscriptions, self(), requested) == %{
             changes: MapSet.new(),
             wire_events: MapSet.new()
           }

    assert EventSubscriptions.plan_unsubscribe(subscriptions, self(), []) == %{
             changes: changes,
             wire_events: MapSet.new(["FLOW_WAKE"])
           }
  end

  test "malformed and oversized server event names are ignored without raising" do
    assert EventSubscriptions.event_kind(%{"event" => <<0xFF>>}) == nil
    assert EventSubscriptions.event_kind(%{"kind" => String.duplicate("x", 10_000)}) == nil
    assert EventSubscriptions.event_kind(%{event: "flow_wake"}) == "FLOW_WAKE"
    assert EventSubscriptions.event_kind(%{event: "NOT_SUPPORTED"}) == nil
  end
end
