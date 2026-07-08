defmodule FerricStore.FlowTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Protocol

  defmodule CaptureNativeClient do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:native, opcode, payload, opts}, _from, test_pid) do
      send(test_pid, {:native, opcode, payload, opts})

      reply =
        case opcode do
          unquote(FerricStore.Protocol.opcode(:flow_claim_due)) ->
            [
              ["flow-1", "tenant:a", "lease-1", 10],
              ["flow-2", "tenant:b", "lease-2", 11, %{"tenant" => "acme"}]
            ]

          _other ->
            "OK"
        end

      {:reply, reply, test_pid}
    end
  end

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
        state_meta: %{version: 1, owner: "risk"},
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
             "state_meta" => %{"version" => 1, "owner" => "risk"},
             "values" => %{"prompt" => "hi"},
             "value_refs" => %{"large" => "ref-1"}
           }
  end

  test "builds create args with state metadata" do
    args =
      Flow.create_args("flow-1",
        type: "email",
        state: "accept",
        state_meta: %{version: 1, owner: "risk"},
        now_ms: 10
      )

    assert Enum.slice(args, -6, 6) == [
             "STATE_META",
             "owner",
             "risk",
             "STATE_META",
             "version",
             1
           ]
  end

  test "builds direct create many payload" do
    assert Flow.create_many_payload(["flow-1", {"flow-2", "payload"}],
             type: "email",
             now_ms: 10,
             independent: true,
             return_ok_on_success: true
           ) == %{
             "items" => [["flow-1", ""], ["flow-2", "payload"]],
             "type" => "email",
             "state" => "queued",
             "now_ms" => 10,
             "run_at_ms" => 10,
             "independent" => true,
             "return" => "OK_ON_SUCCESS"
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

  test "builds direct claim due payload" do
    payload =
      Flow.claim_due_payload("email",
        state: "queued",
        worker: "w1",
        now_ms: 100,
        limit: 10,
        include_attributes: false,
        partition_keys: ["p1", "p2"]
      )

    assert payload == %{
             "type" => "email",
             "state" => "queued",
             "worker" => "w1",
             "lease_ms" => 30_000,
             "limit" => 10,
             "now_ms" => 100,
             "return" => "JOBS_COMPACT",
             "partition_keys" => ["p1", "p2"]
           }
  end

  test "claim due command args support explicit partition filters" do
    args =
      Flow.claim_due_args("email",
        state: "queued",
        worker: "w1",
        now_ms: 100,
        partition_keys: ["p1", "p2"],
        include_attributes: false
      )

    assert args == [
             "email",
             "STATE",
             "queued",
             "WORKER",
             "w1",
             "LEASE_MS",
             30_000,
             "LIMIT",
             1,
             "NOW",
             100,
             "PARTITIONS",
             2,
             "p1",
             "p2",
             "RETURN",
             "JOBS_COMPACT"
           ]
  end

  test "claim due normalizes compact jobs with partition, lease, and fencing fields" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert [
             %{
               "id" => "flow-1",
               "partition_key" => "tenant:a",
               "lease_token" => "lease-1",
               "fencing_token" => 10
             },
             %{
               "id" => "flow-2",
               "partition_key" => "tenant:b",
               "lease_token" => "lease-2",
               "fencing_token" => 11,
               "attributes" => %{"tenant" => "acme"}
             }
           ] =
             Flow.claim_due(client, "email",
               state: "queued",
               worker: "w1",
               partition_keys: ["tenant:a", "tenant:b"],
               include_attributes: false
             )

    assert_received {:native, opcode,
                     %{
                       "type" => "email",
                       "state" => "queued",
                       "worker" => "w1",
                       "partition_keys" => ["tenant:a", "tenant:b"]
                     }, []}

    assert opcode == Protocol.opcode(:flow_claim_due)
  end

  test "builds transition args" do
    args =
      Flow.transition_args("flow-1",
        from_state: "queued",
        to_state: "sent",
        lease_token: "token",
        fencing_token: 3,
        payload: "next",
        state_meta: %{version: 2},
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
             100,
             "STATE_META",
             "version",
             2
           ]
  end

  test "builds direct transition payload with partition and fencing data" do
    assert Flow.transition_payload("flow-1",
             from_state: "running",
             to_state: "ready",
             partition_key: "tenant:a",
             lease_token: "lease-1",
             fencing_token: 7,
             payload: "next",
             state_meta: %{version: 2},
             now_ms: 100,
             run_at_ms: 200
           ) == %{
             "id" => "flow-1",
             "from_state" => "running",
             "to_state" => "ready",
             "partition_key" => "tenant:a",
             "lease_token" => "lease-1",
             "fencing_token" => 7,
             "payload" => "next",
             "state_meta" => %{"version" => 2},
             "now_ms" => 100,
             "run_at_ms" => 200
           }
  end

  test "transition sends typed native payload" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert Flow.transition(client, "flow-1",
             from_state: "running",
             to_state: "ready",
             partition_key: "tenant:a",
             lease_token: "lease-1",
             fencing_token: 7,
             now_ms: 100
           ) == "OK"

    assert_received {:native, opcode,
                     %{
                       "id" => "flow-1",
                       "from_state" => "running",
                       "to_state" => "ready",
                       "partition_key" => "tenant:a",
                       "lease_token" => "lease-1",
                       "fencing_token" => 7
                     }, []}

    assert opcode == Protocol.opcode(:flow_transition)
  end

  test "builds direct complete payload" do
    assert Flow.complete_payload("flow-1",
             lease_token: "token",
             fencing_token: 5,
             result: "ok",
             state_meta: %{version: 3},
             now_ms: 10
           ) == %{
             "id" => "flow-1",
             "lease_token" => "token",
             "fencing_token" => 5,
             "result" => "ok",
             "now_ms" => 10,
             "state_meta" => %{"version" => 3}
           }
  end

  test "builds flow policy and search payloads for indexed state metadata" do
    assert Flow.policy_set_payload("review", indexed_state_meta: "version") == %{
             "type" => "review",
             "indexed_state_meta" => "version"
           }

    assert Flow.policy_set_payload("review", indexed_state_meta: nil) == %{
             "type" => "review",
             "indexed_state_meta" => nil
           }

    assert Flow.search_payload(
             type: "review",
             state: "accept",
             partition_key: "tenant-1",
             state_meta: %{version: 1},
             consistent_projection: true,
             terminal_only: true,
             count: 10
           ) == %{
             "type" => "review",
             "state" => "accept",
             "partition_key" => "tenant-1",
             "state_meta" => %{"accept" => %{"version" => 1}},
             "consistent_projection" => true,
             "terminal_only" => true,
             "count" => 10
           }
  end

  test "builds state policy payloads for FIFO and parallel modes" do
    assert Flow.policy_set_args("review",
             states: [
               {"queued", [mode: :fifo]},
               {"reviewing", %{mode: :parallel}}
             ]
           ) == [
             "review",
             "STATE",
             "queued",
             "MODE",
             "FIFO",
             "STATE",
             "reviewing",
             "MODE",
             "PARALLEL"
           ]

    assert Flow.policy_set_payload("review",
             indexed_attributes: ["tenant", "region"],
             retry: [max_retries: 5, backoff: [kind: :exponential, base_ms: 100]],
             retention: [ttl_ms: 60_000],
             states: %{
               "queued" => [mode: :fifo],
               "reviewing" => %{mode: :parallel}
             }
           ) == %{
             "type" => "review",
             "indexed_attributes" => ["tenant", "region"],
             "retry" => %{
               "max_retries" => 5,
               "backoff" => %{"kind" => :exponential, "base_ms" => 100}
             },
             "retention" => %{"ttl_ms" => 60_000},
             "states" => %{
               "queued" => %{"mode" => :fifo},
               "reviewing" => %{"mode" => :parallel}
             }
           }

    assert Flow.policy_set_payload("review", []) == %{"type" => "review"}

    assert Flow.policy_get_payload("review", state: "queued") == %{
             "type" => "review",
             "state" => "queued"
           }
  end

  test "builds direct complete many payload" do
    jobs = [
      %{"id" => "flow-1", "partition_key" => "p1", "lease_token" => "l1", "fencing_token" => 1},
      %{"id" => "flow-2", "lease_token" => "l2", "fencing_token" => 2}
    ]

    assert Flow.complete_many_payload(jobs, now_ms: 10, return_ok_on_success: true) == %{
             "items" => [["flow-1", "p1", "l1", 1], ["flow-2", "l2", 2]],
             "now_ms" => 10,
             "return" => "OK_ON_SUCCESS"
           }
  end
end
