defmodule FerricStore.SDK.Native.EventRequestTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.SDK.Native.EventRequest

  test "builds subscription requests with one canonical shape" do
    event_call = %{action: :subscribe, from: :from, opts: :opts}
    changes = MapSet.new(["FLOW_WAKE"])

    assert %{
             kind: :event_subscribe,
             opcode: subscribe_opcode,
             payload: %{"events" => ["FLOW_WAKE"]},
             event_call: ^event_call,
             event_changes: ^changes
           } = EventRequest.operation(event_call, changes, changes)

    assert subscribe_opcode == CommandSpec.fetch!(:subscribe_events).opcode
  end

  test "restore retries use a bounded exponential backoff" do
    assert EventRequest.restore_backoff(1) == 50
    assert EventRequest.restore_backoff(2) == 100
    assert EventRequest.restore_backoff(100) == 1_000
  end
end
