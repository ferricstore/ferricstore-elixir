defmodule FerricStore.SDK.Native.ClientPolicyTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{Admission, BatchPolicy, EventRouter}

  test "logical admission excludes internal batch wire groups" do
    admission = %Admission{batch_groups: 2}

    refute Admission.full?(admission, 3, 0, 0, 0, 0, 2)
    assert Admission.full?(admission, 4, 0, 0, 0, 0, 2)
    assert Admission.full?(admission, 3, 0, 0, 1, 0, 2)
    assert Admission.full?(admission, 2, 0, 0, 0, 2, 2)
    assert Admission.wire_slots(4, 3) == 1
    assert Admission.adjust_batch_groups(admission, -10) == %Admission{batch_groups: 0}
  end

  test "batch completion retries only all-failure retryable outcomes" do
    batch = %{attempt: 0, opcode: 0x0101, opts: RequestContext.new([], 100)}

    failures = [
      %{indexes: [0], reason: {:connect_failed, :first}},
      %{indexes: [1], reason: {:connect_failed, :second}}
    ]

    assert BatchPolicy.completion(batch, [], failures) ==
             {:retry, {:group_failures, [{:connect_failed, :first}, {:connect_failed, :second}]}}

    successes = [%{indexes: [0], value: "ok"}]

    assert {:error, {:partial_group_failure, %{successes: ^successes, failures: ^failures}}} =
             BatchPolicy.completion(batch, successes, failures)
  end

  test "event routing always includes the client identity" do
    client = self()
    event = %{value: %{"event" => "FLOW_WAKE"}}
    subscribers = %{self() => %{events: MapSet.new(["FLOW_WAKE"])}}

    assert :ok = EventRouter.deliver(client, subscribers, event, "FLOW_WAKE")
    assert_receive {:ferricstore_event, ^client, ^event}
    refute_receive {:ferricstore_event, ^event}
  end
end
