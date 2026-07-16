defmodule FerricStore.SDK.Native.EventCallTest do
  use ExUnit.Case, async: true

  alias FerricStore.{DeadlineBudget, RequestContext}
  alias FerricStore.SDK.Native.{EventCall, EventSubscriptions}

  test "public calls are typed and monitor only their caller" do
    context = RequestContext.new([], 500)
    from = {self(), make_ref()}

    call = EventCall.new(:subscribe, self(), ["goaway"], context, from)

    assert %EventCall{
             action: :subscribe,
             subscriber: subscriber,
             events: ["goaway"],
             from: ^from,
             caller_monitor: monitor,
             subscriber_down: false
           } = call

    assert subscriber == self()
    assert is_reference(monitor)
    Process.demonitor(monitor, [:flush])
  end

  test "subscriber cleanup has no waiting caller" do
    assert %EventCall{
             action: :unsubscribe,
             subscriber_down: true,
             from: nil,
             caller_monitor: nil
           } = EventCall.subscriber_down(self(), 500)
  end

  test "dequeue gives internal subscriber cleanup a fresh wire deadline" do
    cleanup = EventCall.subscriber_down(self(), 500)

    expired_context = %{
      cleanup.opts
      | deadline: %DeadlineBudget{expires_at: System.monotonic_time(:millisecond) - 1}
    }

    cleanup = %{cleanup | opts: expired_context}
    assert {:error, :timeout} = RequestContext.ensure_active(cleanup.opts)

    dequeued = EventCall.dequeued(cleanup)
    assert :ok = RequestContext.ensure_active(dequeued.opts)
  end

  test "planning distinguishes no-op, local, and wire transitions" do
    subscriber = self()
    context = RequestContext.new([], 500)
    subscriptions = EventSubscriptions.new()
    subscribe = EventCall.new(:subscribe, subscriber, ["goaway"], context, nil)

    assert {:wire, changes, wire_events} = EventCall.plan(subscribe, subscriptions)
    assert wire_events == changes

    subscriptions = EventSubscriptions.subscribe(subscriptions, subscriber, changes, nil)
    assert {:noop, empty} = EventCall.plan(subscribe, subscriptions)
    assert MapSet.size(empty) == 0

    second = EventCall.new(:subscribe, spawn(fn -> :ok end), ["goaway"], context, nil)
    assert {:local, ^changes} = EventCall.plan(second, subscriptions)
  end
end
