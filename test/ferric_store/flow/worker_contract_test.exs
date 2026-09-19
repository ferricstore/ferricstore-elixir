defmodule FerricStore.Flow.WorkerContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Flow
  alias FerricStore.Flow.{ClaimResponseDecoder, PolicyCommand}

  test "retry policies retain nested type and state backoff fields, including zeros" do
    retry = [
      max_retries: 1,
      backoff: [kind: "none", base_ms: 0, max_ms: 0, jitter_pct: 0],
      exhausted_to: "failed"
    ]

    assert {:ok, payload} =
             PolicyCommand.set_payload("worker-contract",
               retry: retry,
               states: %{"queued" => [mode: "fifo", retry: retry]}
             )

    expected = %{
      "max_retries" => 1,
      "backoff" => %{"kind" => "none", "base_ms" => 0, "max_ms" => 0, "jitter_pct" => 0},
      "exhausted_to" => "failed"
    }

    assert payload["retry"] == expected
    assert payload["states"]["queued"]["retry"] == expected
    refute Map.has_key?(payload, "max_retries")
  end

  test "record claims explicitly request records and preserve selected value names" do
    assert %{"return" => "RECORDS", "values" => ["checkpoint"], "payload" => true} =
             Flow.claim_due_payload("worker-contract",
               worker: "worker",
               include_record: true,
               payload: true,
               values: ["checkpoint"]
             )

    assert %{"return" => "JOBS_COMPACT_ATTRS"} =
             Flow.claim_due_payload("worker-contract", worker: "worker")
  end

  test "full record decoding preserves metadata and decodes only selected binary values" do
    record = %{
      "id" => "flow",
      "partition_key" => "partition",
      "lease_token" => "lease",
      "fencing_token" => 2,
      "version" => 7,
      "attempts" => 1,
      "state_meta" => %{"queued" => %{"phase" => "ready"}},
      "payload" => Term.encode(%{body: "payload"}),
      "values" => %{"checkpoint" => Term.encode(%{step: 2})}
    }

    assert [decoded] = ClaimResponseDecoder.decode([record], Term)
    assert decoded["payload"] == %{body: "payload"}
    assert decoded["values"] == %{"checkpoint" => %{step: 2}}
    assert Map.drop(decoded, ["payload", "values"]) == Map.drop(record, ["payload", "values"])
  end
end
