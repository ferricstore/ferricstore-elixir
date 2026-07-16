defmodule FerricStore.SDK.Native.EventFanoutTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.EventFanout

  test "fanout delivery is isolated and preserves subscriber filtering" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    events = MapSet.new(["FLOW_WAKE"])
    :ok = EventFanout.subscribe(fanout, self(), events)
    :ok = EventFanout.sync(fanout)

    event = %{value: %{"event" => "FLOW_WAKE"}}
    assert :ok = EventFanout.dispatch(fanout, event, "FLOW_WAKE")
    assert_receive {:ferricstore_event, client, ^event}
    assert client == self()

    assert :ok = EventFanout.stop(fanout)
  end

  test "a suspended fanout has bounded non-blocking admission" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    :ok = :sys.suspend(fanout.pid)

    results =
      Enum.map(1..100, fn index ->
        EventFanout.dispatch(fanout, %{value: index}, :broadcast)
      end)

    assert Enum.count(results, &(&1 == :ok)) == 8
    assert Enum.count(results, &(&1 == :dropped)) == 92
    assert EventFanout.pending(fanout) == 8

    :ok = :sys.resume(fanout.pid)
    assert_eventually(fn -> EventFanout.pending(fanout) == 0 end)
    assert :ok = EventFanout.stop(fanout)
  end

  test "mailbox messages cannot forge delivery or corrupt pending admission" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    :ok = EventFanout.subscribe(fanout, self(), MapSet.new(["FLOW_WAKE"]))
    :ok = EventFanout.sync(fanout)
    event = %{value: %{"event" => "FLOW_WAKE"}}

    send(fanout.pid, {EventFanout, :deliver, event, :broadcast})

    assert :ok = EventFanout.sync(fanout)
    refute_receive {:ferricstore_event, _, _}
    assert EventFanout.pending(fanout) == 0
    assert Process.alive?(fanout.pid)
    assert :ok = EventFanout.stop(fanout)
  end

  test "mailbox messages cannot forge subscriptions or remove subscribers" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    events = MapSet.new(["FLOW_WAKE"])
    event = %{value: %{"event" => "FLOW_WAKE"}}

    send(fanout.pid, {EventFanout, :subscribe, self(), events})
    assert :ok = EventFanout.sync(fanout)
    assert :ok = EventFanout.dispatch(fanout, event, "FLOW_WAKE")
    refute_receive {:ferricstore_event, _, _}

    :ok = EventFanout.subscribe(fanout, self(), events)
    assert :ok = EventFanout.sync(fanout)
    send(fanout.pid, {EventFanout, :remove_subscriber, self()})
    assert :ok = EventFanout.sync(fanout)
    assert :ok = EventFanout.dispatch(fanout, event, "FLOW_WAKE")
    assert_receive {:ferricstore_event, client, ^event}
    assert client == self()

    assert :ok = EventFanout.stop(fanout)
  end

  defp assert_eventually(fun, attempts \\ 40)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())
end
