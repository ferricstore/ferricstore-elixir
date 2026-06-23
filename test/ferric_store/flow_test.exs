defmodule FerricStore.FlowTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow

  test "builds create args with payload, attributes, and value refs" do
    args =
      Flow.create_args("flow-1",
        type: "email",
        state: "queued",
        payload: "hello",
        attributes: %{tenant: "acme"},
        values: %{prompt: "hi"},
        value_refs: %{large: "ref-1"},
        partition_key: "p1",
        now_ms: 10,
        run_at_ms: 20
      )

    assert args == [
             "flow-1",
             "TYPE",
             "email",
             "STATE",
             "queued",
             "NOW",
             10,
             "PARTITION",
             "p1",
             "PAYLOAD",
             "hello",
             "RUN_AT",
             20,
             "ATTRIBUTE",
             "tenant",
             "acme",
             "VALUE",
             "prompt",
             "hi",
             "VALUE_REF",
             "large",
             "ref-1"
           ]
  end

  test "builds direct create payload with encoded values and string keys" do
    payload =
      Flow.create_payload("flow-1",
        type: "email",
        payload: "hello",
        attributes: %{tenant: "acme"},
        values: %{prompt: "hi"},
        value_refs: %{large: "ref-1"},
        now_ms: 10
      )

    assert payload == %{
             "id" => "flow-1",
             "type" => "email",
             "state" => "queued",
             "now_ms" => 10,
             "run_at_ms" => 10,
             "payload" => "hello",
             "attributes" => %{"tenant" => "acme"},
             "values" => %{"prompt" => "hi"},
             "value_refs" => %{"large" => "ref-1"}
           }
  end

  test "claim due defaults to compact jobs with attributes" do
    args = Flow.claim_due_args("email", state: "queued", worker: "w1", now_ms: 100, limit: 10)

    assert args == [
             "email",
             "STATE",
             "queued",
             "WORKER",
             "w1",
             "LEASE_MS",
             30_000,
             "LIMIT",
             10,
             "NOW",
             100,
             "RETURN",
             "JOBS_COMPACT_ATTRS"
           ]
  end

  test "builds transition args" do
    args =
      Flow.transition_args("flow-1",
        from_state: "queued",
        to_state: "sent",
        lease_token: "token",
        fencing_token: 3,
        payload: "next",
        now_ms: 100
      )

    assert args == [
             "flow-1",
             "queued",
             "sent",
             "LEASE_TOKEN",
             "token",
             "FENCING",
             3,
             "NOW",
             100,
             "PAYLOAD",
             "next",
             "RUN_AT",
             100
           ]
  end

  test "builds direct complete payload" do
    assert Flow.complete_payload("flow-1",
             lease_token: "token",
             fencing_token: 5,
             result: "ok",
             now_ms: 10
           ) == %{
             "id" => "flow-1",
             "lease_token" => "token",
             "fencing_token" => 5,
             "result" => "ok",
             "now_ms" => 10
           }
  end
end
