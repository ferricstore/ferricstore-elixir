defmodule FerricStore.FlowWorkerContractIntegrationTest do
  use ExUnit.Case, async: false

  alias FerricStore.Codec.Term
  alias FerricStore.Flow
  alias FerricStore.Flow.PolicySnapshot
  alias FerricStore.SDK
  alias FerricStore.SDK.Flow, as: NativeFlow

  @moduletag :integration
  @url System.get_env("FERRICSTORE_TEST_URL", "ferric://127.0.0.1:6388")

  setup do
    {:ok, client} = SDK.start_link(url: @url, endpoint_policy: :any)
    on_exit(fn -> SDK.close(client) end)

    scope =
      "elixir-worker-contract-#{System.unique_integer([:positive])}-#{System.system_time(:microsecond)}"

    %{client: client, scope: scope, now: System.system_time(:millisecond)}
  end

  test "type and state retry snapshots round-trip exact policy values", %{
    client: client,
    scope: type
  } do
    retry = %{
      "max_retries" => 1,
      "backoff" => %{"kind" => "none", "base_ms" => 0, "max_ms" => 0, "jitter_pct" => 0},
      "exhausted_to" => "failed"
    }

    state_retry = %{
      "max_retries" => 2,
      "backoff" => %{"kind" => "fixed", "base_ms" => 7, "max_ms" => 19, "jitter_pct" => 3},
      "exhausted_to" => "failed"
    }

    assert %PolicySnapshot{} =
             snapshot =
             Flow.policy_set(client, type,
               replace: true,
               retry: retry,
               states: %{"queued" => %{mode: "fifo", retry: state_retry}}
             )

    assert snapshot.retry == retry
    assert snapshot.states["queued"]["retry"] == state_retry
    assert %PolicySnapshot{} = fetched = Flow.policy_get(client, type)
    assert fetched.retry == retry
    assert fetched.states["queued"]["retry"] == state_retry
  end

  test "claim and reclaim preserve complete records and selected values", context do
    %{client: client, scope: scope, now: now} = context

    assert "OK" =
             Flow.create(client, scope,
               type: scope,
               partition_key: scope,
               now_ms: now,
               run_at_ms: now,
               payload: %{body: "payload"},
               values: %{checkpoint: %{step: 2}},
               attributes: %{source: "sdk"},
               state_meta: %{phase: "ready"},
               codec: Term
             )

    assert [record] =
             Flow.claim_due(client, scope,
               worker: "elixir-claim",
               partition_key: scope,
               now_ms: now + 1,
               lease_ms: 1_000,
               include_record: true,
               payload: true,
               values: ["checkpoint"],
               codec: Term
             )

    assert record["id"] == scope
    assert record["type"] == scope
    assert record["version"] == 2
    assert record["attempts"] == 0
    assert record["payload"] == %{body: "payload"}
    assert record["values"] == %{"checkpoint" => %{step: 2}}
    assert record["state_meta"] == %{"queued" => %{"phase" => "ready"}}
    assert record["attributes"] == %{"source" => "sdk"}

    assert {:ok, [reclaimed]} =
             NativeFlow.reclaim(client, %{
               type: scope,
               partition_key: scope,
               worker: "elixir-reclaim",
               limit: 1,
               now_ms: now + 2_000,
               lease_ms: 1_000,
               return: "RECORDS",
               payload: true,
               values: ["checkpoint"]
             })

    assert reclaimed["id"] == scope
    assert reclaimed["fencing_token"] > record["fencing_token"]
    assert reclaimed["lease_token"] != record["lease_token"]
    assert Term.decode(reclaimed["payload"]) == record["payload"]
    assert Term.decode(reclaimed["values"]["checkpoint"]) == %{step: 2}
    assert reclaimed["state_meta"] == record["state_meta"]
  end

  test "native rewind persists binary reason and leaves a claimable successor", context do
    %{client: client, scope: scope, now: now} = context

    assert {:ok, "OK"} =
             NativeFlow.create(client, %{
               id: scope,
               type: scope,
               state: "queued",
               partition_key: scope,
               now_ms: now,
               run_at_ms: now
             })

    assert {:ok, [[event_id, _fields]]} =
             NativeFlow.history(client, %{id: scope, partition_key: scope})

    assert [job] =
             Flow.claim_due(client, scope,
               worker: "elixir-rewind",
               partition_key: scope,
               now_ms: now + 1
             )

    assert "OK" =
             Flow.transition(client, scope,
               from_state: "running",
               to_state: "ready",
               partition_key: scope,
               now_ms: now + 2,
               lease_token: job["lease_token"],
               fencing_token: job["fencing_token"]
             )

    reason = <<0, 255, 1, 254>> <> "operator rewind"

    assert {:ok, "OK"} =
             NativeFlow.rewind(client, %{
               id: scope,
               partition_key: scope,
               to_event: event_id,
               expect_state: "ready",
               now_ms: now + 3,
               reason: reason
             })

    assert {:ok, record} = NativeFlow.get(client, %{id: scope, partition_key: scope})
    assert record["state"] == "queued"
    assert is_binary(record["error_ref"])
    assert {:ok, [^reason]} = NativeFlow.value_mget(client, %{refs: [record["error_ref"]]})

    assert [next_job] =
             Flow.claim_due(client, scope,
               worker: "elixir-after-rewind",
               partition_key: scope,
               now_ms: now + 4
             )

    assert next_job["id"] == scope
    assert next_job["fencing_token"] > job["fencing_token"]
  end
end
