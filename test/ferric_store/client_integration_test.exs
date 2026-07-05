defmodule FerricStore.ClientIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @docker_url System.get_env("FERRICSTORE_TEST_URL", "ferric://127.0.0.1:6388")

  setup do
    client = FerricStore.connect!(url: @docker_url, client_name: "ferricstore-elixir-test")

    on_exit(fn -> FerricStore.close(client) end)

    {:ok, client: client}
  end

  test "KV helpers cover set, get, mset, mget, and delete", %{client: client} do
    prefix = unique("kv")
    key = "#{prefix}:one"
    other_key = "#{prefix}:two"

    assert :ok = FerricStore.set(client, key, "value")
    assert FerricStore.get(client, key) == "value"

    assert FerricStore.mset(client, %{key => "value-2", other_key => "other"}) in ["OK", :ok]

    assert FerricStore.mget(client, [key, other_key, "#{prefix}:missing"]) == [
             "value-2",
             "other",
             nil
           ]

    assert_integer_like(FerricStore.delete(client, [key, other_key]), 2)
  end

  test "one client multiplexes concurrent requests", %{client: client} do
    prefix = unique("mux")

    1..50
    |> Enum.map(fn index ->
      Task.async(fn -> FerricStore.set(client, "#{prefix}:#{index}", "v#{index}") end)
    end)
    |> Task.await_many(30_000)
    |> Enum.each(fn value -> assert value == :ok end)

    values =
      1..50
      |> Enum.map(fn index ->
        Task.async(fn -> FerricStore.get(client, "#{prefix}:#{index}") end)
      end)
      |> Task.await_many(30_000)

    assert values == Enum.map(1..50, &"v#{&1}")
  end

  test "one client multiplexes async refs without spawning callers", %{client: client} do
    prefix = unique("async")

    refs =
      Enum.map(1..50, fn index ->
        FerricStore.async_native(
          client,
          FerricStore.Protocol.opcode(:set),
          %{"key" => "#{prefix}:#{index}", "value" => "v#{index}"}
        )
      end)

    refs
    |> Enum.map(&FerricStore.await(&1, 30_000))
    |> Enum.each(fn response -> assert response == "OK" end)

    refs =
      Enum.map(1..50, fn index ->
        FerricStore.async_native(
          client,
          FerricStore.Protocol.opcode(:get),
          %{"key" => "#{prefix}:#{index}"}
        )
      end)

    values = Enum.map(refs, &FerricStore.await(&1, 30_000))
    assert values == Enum.map(1..50, &"v#{&1}")
  end

  test "hash helpers cover hset, hget, hmget, and hgetall", %{client: client} do
    key = unique("hash")

    assert_okish(FerricStore.hset(client, key, "field-1", "value-1"))
    assert_okish(FerricStore.hset(client, key, "field-2", "value-2"))

    assert FerricStore.hget(client, key, "field-1") == "value-1"

    assert FerricStore.hmget(client, key, ["field-1", "field-2", "missing"]) == [
             "value-1",
             "value-2",
             nil
           ]

    assert hgetall_field(FerricStore.hgetall(client, key), "field-2") == "value-2"
  end

  test "list helpers cover lpush, rpush, lpop, rpop, and lrange", %{client: client} do
    key = unique("list")

    assert_integer_like(FerricStore.lpush(client, key, ["a", "b"]))
    assert_integer_like(FerricStore.rpush(client, key, "c"))

    assert FerricStore.lrange(client, key, 0, -1) == ["b", "a", "c"]
    assert FerricStore.lpop(client, key) == "b"
    assert FerricStore.rpop(client, key) == "c"
    assert FerricStore.lrange(client, key, 0, -1) == ["a"]
  end

  test "set helpers cover sadd, srem, smembers, and sismember", %{client: client} do
    key = unique("set")

    assert_integer_like(FerricStore.sadd(client, key, ["a", "b"]))
    assert FerricStore.sismember(client, key, "a") in [1, true, "1"]
    assert Enum.sort(FerricStore.smembers(client, key)) == ["a", "b"]
    assert_integer_like(FerricStore.srem(client, key, "a"))
    refute FerricStore.sismember(client, key, "a") in [1, true, "1"]
  end

  test "sorted set helpers cover zadd, zrange, zscore, and zrem", %{client: client} do
    key = unique("zset")

    assert_okish(FerricStore.zadd(client, key, 1, "a"))
    assert_okish(FerricStore.zadd(client, key, 2, "b"))

    assert FerricStore.zrange(client, key, 0, -1) == ["a", "b"]
    assert FerricStore.zscore(client, key, "a") in [1, 1.0, "1", "1.0"]
    assert_integer_like(FerricStore.zrem(client, key, "a"))
    assert FerricStore.zrange(client, key, 0, -1) == ["b"]
  end

  test "flow lifecycle covers create, value refs, get, list, history, claim, transition, and complete",
       %{
         client: client
       } do
    id = unique("flow")
    type = unique("type")
    worker = unique("worker")

    ref = FerricStore.Flow.value_put(client, "large-value")

    assert is_binary(ref) or is_map(ref)

    assert FerricStore.Flow.create(client, id,
             type: type,
             payload: "payload",
             attributes: %{tenant: "acme"},
             value_refs: %{blob: extract_ref(ref)},
             now_ms: System.system_time(:millisecond)
           ) in ["OK", "QUEUED", "CREATED"]

    assert_value_mget(client, ref, "large-value")
    assert is_map(FerricStore.Flow.get(client, id, payload: true))
    assert is_list(FerricStore.Flow.list(client, type: type, state: "queued", count: 10))
    assert is_list(FerricStore.Flow.history(client, id))

    [job | _] = claim_one(client, type, "queued", worker)
    assert Map.get(job, "attributes", %{})["tenant"] == "acme"

    assert_okish(
      FerricStore.Flow.transition(client, id,
        from_state: "running",
        to_state: "processing",
        partition_key: Map.get(job, "partition_key"),
        lease_token: Map.fetch!(job, "lease_token"),
        fencing_token: Map.fetch!(job, "fencing_token"),
        payload: "next",
        attributes: %{step: "processing"}
      )
    )

    [processing_job | _] = claim_one(client, type, "processing", worker)

    assert_okish(
      FerricStore.Flow.complete(client, id,
        partition_key: Map.get(processing_job, "partition_key"),
        lease_token: Map.fetch!(processing_job, "lease_token"),
        fencing_token: Map.fetch!(processing_job, "fencing_token"),
        result: "done"
      )
    )
  end

  test "flow many helpers cover create_many and complete_many", %{client: client} do
    type = unique("many-type")
    worker = unique("many-worker")
    ids = Enum.map(1..3, &"#{type}:#{&1}")

    assert_okish(
      FerricStore.Flow.create_many(client, ids,
        type: type,
        independent: true,
        return_ok_on_success: true
      )
    )

    jobs =
      FerricStore.Flow.claim_due(client, type,
        state: "queued",
        worker: worker,
        limit: 3,
        include_attributes: false
      )

    assert length(jobs) == 3

    assert_okish(
      FerricStore.Flow.complete_many(client, jobs,
        result: "done",
        return_ok_on_success: true
      )
    )
  end

  test "flow state metadata policy, readback, and search use the Docker server", %{
    client: client
  } do
    suffix = unique("state-meta")
    type = "#{suffix}-type"
    partition = "#{suffix}-partition"
    id = "#{suffix}-flow"

    policy = FerricStore.Flow.policy_set(client, type, indexed_state_meta: "version")
    assert policy["indexed_state_meta"] == "version"

    assert FerricStore.Flow.policy_get(client, type)["indexed_state_meta"] == "version"

    assert_okish(
      FerricStore.Flow.create(client, id,
        type: type,
        state: "accept",
        partition_key: partition,
        state_meta: %{version: 1, owner: "risk"},
        now_ms: System.system_time(:millisecond)
      )
    )

    flow = FerricStore.Flow.get(client, id, partition_key: partition)
    assert state_meta_value(flow, "accept", "version") == 1
    assert state_meta_value(flow, "accept", "owner") == "risk"

    assert_eventually(fn ->
      records =
        FerricStore.Flow.search(
          client,
          type: type,
          partition_key: partition,
          state: "accept",
          state_meta: %{version: 1},
          consistent_projection: true,
          count: 10
        )

      assert Enum.any?(records, &(&1["id"] == id))
    end)
  end

  test "topology-aware SDK flow wrapper supports indexed state metadata", %{client: old_client} do
    {:ok, client} =
      FerricStore.SDK.start_link(
        url: @docker_url,
        client_name: "ferricstore-elixir-sdk-test",
        endpoint_policy: :any
      )

    on_exit(fn -> FerricStore.SDK.close(client) end)

    suffix = unique("sdk-state-meta")
    type = "#{suffix}-type"
    partition = "#{suffix}-partition"
    id = "#{suffix}-flow"

    assert {:ok, policy} =
             FerricStore.SDK.Flow.policy_set(client, %{
               type: type,
               indexed_state_meta: "version"
             })

    assert policy["indexed_state_meta"] == "version"

    assert {:ok, "OK"} =
             FerricStore.SDK.Flow.create(client, %{
               id: id,
               type: type,
               state: "accept",
               partition_key: partition,
               state_meta: %{version: 1, owner: "risk"},
               now_ms: System.system_time(:millisecond)
             })

    assert {:ok, flow} =
             FerricStore.SDK.Flow.get(client, %{id: id, partition_key: partition, full: true})

    assert state_meta_value(flow, "accept", "version") == 1

    assert_eventually(fn ->
      assert {:ok, records} =
               FerricStore.SDK.Flow.search(client, %{
                 type: type,
                 partition_key: partition,
                 state_meta: %{accept: %{version: 1}},
                 consistent_projection: true,
                 count: 10
               })

      assert Enum.any?(records, &(&1["id"] == id))
    end)

    assert FerricStore.ping(old_client) == "PONG"
  end

  test "flow terminal helpers cover retry, fail, and cancel", %{client: client} do
    retry_type = unique("retry-type")
    retry_worker = unique("retry-worker")
    retry_id = unique("retry-flow")

    assert_okish(FerricStore.Flow.create(client, retry_id, type: retry_type, payload: "payload"))
    [retry_job | _] = claim_one(client, retry_type, "queued", retry_worker)

    assert_okish(
      FerricStore.Flow.retry(client, retry_id,
        partition_key: Map.get(retry_job, "partition_key"),
        lease_token: Map.fetch!(retry_job, "lease_token"),
        fencing_token: Map.fetch!(retry_job, "fencing_token"),
        error: "try again",
        run_at_ms: System.system_time(:millisecond)
      )
    )

    [failed_job | _] = claim_one(client, retry_type, "queued", retry_worker)

    assert_okish(
      FerricStore.Flow.fail(client, retry_id,
        partition_key: Map.get(failed_job, "partition_key"),
        lease_token: Map.fetch!(failed_job, "lease_token"),
        fencing_token: Map.fetch!(failed_job, "fencing_token"),
        error: "failed"
      )
    )

    cancel_type = unique("cancel-type")
    cancel_id = unique("cancel-flow")

    assert_okish(
      FerricStore.Flow.create(client, cancel_id, type: cancel_type, payload: "payload")
    )

    cancel_record = FerricStore.Flow.get(client, cancel_id)

    assert_okish(
      FerricStore.Flow.cancel(client, cancel_id,
        partition_key: Map.get(cancel_record, "partition_key"),
        fencing_token: Map.fetch!(cancel_record, "fencing_token"),
        reason: "cancelled"
      )
    )
  end

  test "queue and workflow convenience APIs use the same native client", %{client: client} do
    queue = FerricStore.Queue.new(client, unique("queue"), worker: unique("queue-worker"))
    queue_id = unique("queue-flow")

    assert_okish(FerricStore.Queue.enqueue(queue, queue_id, payload: "queued"))
    assert [_result] = FerricStore.Queue.run_once(queue, fn _job -> "done" end)

    workflow =
      FerricStore.Workflow.new(client, unique("workflow"),
        initial_state: "reserved",
        worker: unique("workflow-worker")
      )

    workflow_id = unique("workflow-flow")
    assert_okish(FerricStore.Workflow.start(workflow, workflow_id, payload: "started"))

    [job | _] = FerricStore.Workflow.claim(workflow, "reserved", limit: 1)

    assert_okish(
      FerricStore.Workflow.complete(workflow, workflow_id,
        lease_token: Map.fetch!(job, "lease_token"),
        fencing_token: Map.fetch!(job, "fencing_token"),
        result: "done"
      )
    )
  end

  defp extract_ref(%{"ref" => ref}), do: ref
  defp extract_ref(%{ref: ref}), do: ref
  defp extract_ref(ref) when is_binary(ref), do: ref

  defp unique(prefix) do
    "elixir-sdk-#{prefix}-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp claim_one(client, type, state, worker) do
    jobs = FerricStore.Flow.claim_due(client, type, state: state, worker: worker, limit: 1)

    assert is_list(jobs)
    assert [_job | _] = jobs
    jobs
  end

  defp assert_value_mget(client, ref, expected) do
    response = FerricStore.Flow.value_mget(client, [extract_ref(ref)])

    assert response == [expected] or response == [%{"value" => expected}] or
             response == [%{value: expected}]
  end

  defp assert_okish(value) do
    assert value in [:ok, "OK", "QUEUED", "CREATED", "COMPLETED", 1, "1"]
  end

  defp assert_integer_like(value) do
    assert is_integer(value) or (is_binary(value) and match?({_int, ""}, Integer.parse(value)))
  end

  defp assert_integer_like(value, expected) do
    assert value == expected or value == Integer.to_string(expected)
  end

  defp state_meta_value(flow, state, key) do
    flow
    |> Map.fetch!("state_meta")
    |> Map.fetch!(state)
    |> Map.fetch!(key)
  end

  defp assert_eventually(fun, attempts \\ 40, interval_ms \\ 100)

  defp assert_eventually(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [ExUnit.AssertionError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        assert_eventually(fun, attempts - 1, interval_ms)
      end
  end

  defp hgetall_field(%{} = fields, field), do: Map.get(fields, field)

  defp hgetall_field(fields, field) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.find_value(fn
      [^field, value] -> value
      _other -> nil
    end)
  end
end
