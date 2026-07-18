defmodule FerricStore.ClientIntegrationTest do
  use ExUnit.Case, async: false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK
  alias FerricStore.SDK.{Admin, Flow}

  @moduletag :integration
  @docker_url System.get_env("FERRICSTORE_TEST_URL", "ferric://127.0.0.1:6388")

  setup do
    client = FerricStore.connect!(url: @docker_url, client_name: "ferricstore-elixir-test")

    on_exit(fn -> FerricStore.close(client) end)

    {:ok, client: client}
  end

  test "topology-aware SDK control requests cover the Docker native session surface" do
    client = start_sdk_client("control")
    key = unique("sdk-control")

    assert {:ok, "PONG"} = SDK.ping(client)
    assert {:ok, hello} = SDK.request(client, :hello, %{})
    assert hello["protocol"] == "ferricstore-native"

    assert {:error, auth_error} =
             SDK.request(client, :auth, %{
               "username" => unique("missing-auth-user"),
               "password" => "wrong"
             })

    assert_error_message(auth_error, "WRONGPASS")

    assert {:ok, startup} =
             SDK.request(client, :startup, %{"client_name" => unique("sdk-startup")})

    assert startup["protocol"] == "ferricstore-native"

    assert {:ok, "OK"} =
             SDK.request(client, :client_set_name, %{"name" => "ferricstore-sdk-itest"})

    assert {:ok, info} = SDK.request(client, :client_info, %{})
    assert info["client_name"] == "ferricstore-sdk-itest"

    assert {:ok, route} = SDK.request(client, :route, %{"key" => key})
    assert route["key"] == key

    assert {:ok, [batch_route]} = SDK.request(client, :route_batch, %{"keys" => [key]})
    assert batch_route["key"] == key

    assert :ok = SDK.refresh_topology(client)
    assert {:ok, local_route} = SDK.route(client, key)
    assert is_integer(local_route.slot)
    assert %FerricStore.SDK.Native.Topology{} = SDK.topology(client)

    assert {:ok, backpressure} = SDK.request(client, :backpressure, %{})
    assert is_boolean(backpressure["reject_writes"])

    assert {:ok, window} = SDK.request(client, :window_update, %{})
    assert is_boolean(window["accepted"])

    assert {:ok, []} = SDK.request(client, :pipeline, %{"commands" => []})

    assert {:ok, subscribed} = SDK.request(client, :subscribe_events, %{"events" => []})
    assert is_list(subscribed["supported"])

    assert {:ok, unsubscribed} = SDK.request(client, :unsubscribe_events, %{"events" => []})
    assert is_list(unsubscribed["supported"])

    assert {:ok, options} = SDK.request(client, :options, %{})
    assert_sdk_opcode_table_matches(options)
  end

  test "topology-aware SDK KV helpers cover the Docker key command surface" do
    client = start_sdk_client("kv")
    tag = unique("sdk-kv")
    key = same_slot_key(tag, "one")
    other_key = same_slot_key(tag, "two")

    assert {:ok, :ok} = SDK.set(client, key, "value")
    assert {:ok, "value"} = SDK.get(client, key)

    assert {:ok, :ok} = SDK.mset(client, %{key => "value-2", other_key => "other"})

    assert {:ok, ["value-2", "other", nil]} =
             SDK.mget(client, [key, other_key, same_slot_key(tag, "missing")])

    assert {:ok, true} = SDK.cas(client, key, "value-2", "value-3")
    assert {:ok, "value-3"} = SDK.get(client, key)

    owner = unique("owner")
    lock_key = same_slot_key(tag, "lock")

    assert {:ok, _locked} = SDK.lock(client, lock_key, owner, 5_000)
    assert {:ok, _extended} = SDK.extend(client, lock_key, owner, 5_000)
    assert {:ok, _unlocked} = SDK.unlock(client, lock_key, owner)

    assert {:ok, ratelimit} = SDK.ratelimit_add(client, same_slot_key(tag, "rate"), 1_000, 10, 2)
    assert is_list(ratelimit) or is_map(ratelimit)

    compute_key = same_slot_key(tag, "compute")

    assert {:ok, ["compute", _hint, compute_token]} =
             SDK.fetch_or_compute(client, compute_key, 60_000)

    assert {:ok, :ok} =
             SDK.fetch_or_compute_result(
               client,
               compute_key,
               compute_token,
               "computed",
               60_000
             )

    assert {:ok, ["hit", "computed"]} = SDK.fetch_or_compute(client, compute_key, 60_000)

    failed_compute_key = same_slot_key(tag, "compute-error")

    assert {:ok, ["compute", _hint, error_token]} =
             SDK.fetch_or_compute(client, failed_compute_key, 60_000)

    assert {:ok, :ok} =
             SDK.fetch_or_compute_error(client, failed_compute_key, error_token, "failed")

    hash_key = same_slot_key(tag, "hash")

    assert {:ok, _} =
             SDK.hset(client, hash_key, %{"field-1" => "value-1", "field-2" => "value-2"})

    assert {:ok, "value-1"} = SDK.hget(client, hash_key, "field-1")

    assert {:ok, ["value-1", "value-2", nil]} =
             SDK.hmget(client, hash_key, ["field-1", "field-2", "missing"])

    assert {:ok, fields} = SDK.hgetall(client, hash_key)
    assert hgetall_field(fields, "field-2") == "value-2"

    list_key = same_slot_key(tag, "list")
    assert {:ok, _} = SDK.lpush(client, list_key, ["a", "b"])
    assert {:ok, _} = SDK.rpush(client, list_key, "c")
    assert {:ok, ["b", "a", "c"]} = SDK.lrange(client, list_key, 0, -1)
    assert {:ok, "b"} = SDK.lpop(client, list_key)
    assert {:ok, "c"} = SDK.rpop(client, list_key)

    set_key = same_slot_key(tag, "set")
    assert {:ok, _} = SDK.sadd(client, set_key, ["a", "b"])
    assert {:ok, true} = SDK.sismember(client, set_key, "a")
    assert {:ok, members} = SDK.smembers(client, set_key)
    assert Enum.sort(members) == ["a", "b"]
    assert {:ok, _} = SDK.srem(client, set_key, "a")
    assert {:ok, false} = SDK.sismember(client, set_key, "a")

    zset_key = same_slot_key(tag, "zset")
    assert {:ok, _} = SDK.zadd(client, zset_key, [{1, "a"}, {2, "b"}])
    assert {:ok, ["a", "b"]} = SDK.zrange(client, zset_key, 0, -1)
    assert {:ok, 1.0} = SDK.zscore(client, zset_key, "a")
    assert {:ok, _} = SDK.zrem(client, zset_key, "a")
    assert {:ok, ["b"]} = SDK.zrange(client, zset_key, 0, -1)

    assert {:ok, 2} = SDK.del(client, [key, other_key])
  end

  test "topology-aware SDK admin helpers reach Docker and classify unsafe handlers" do
    client = start_sdk_client("admin")
    key = same_slot_key(unique("sdk-admin"), "key")
    assert {:ok, :ok} = SDK.set(client, key, "value")

    classified =
      admin_safe_invocation_cases(key)
      |> Keyword.keys()
      |> MapSet.new()
      |> MapSet.union(MapSet.new(admin_unsafe_invocation_functions()))

    assert classified ==
             MapSet.new(Map.keys(Admin.opcodes()))

    for {function, payload} <- admin_safe_invocation_cases(key) do
      assert_docker_command_response(apply(Admin, function, [client, payload]))
    end

    assert {:ok, health} = Admin.cluster_health(client)
    assert is_binary(health)

    assert {:ok, slot} = Admin.cluster_keyslot(client, %{args: [key]})
    assert is_integer(slot)

    assert {:ok, key_info} = Admin.ferricstore_key_info(client, %{key: key})
    assert is_list(key_info) or is_map(key_info)

    assert {:ok, metrics} = Admin.ferricstore_metrics(client)
    assert is_binary(metrics)
  end

  test "topology-aware SDK management helpers cover the Docker control-plane contract" do
    client = start_sdk_client("management")
    username = "sdk_itest_#{System.unique_integer([:positive])}"
    prefix = unique("mgmt")

    assert {:ok, capabilities} = SDK.capabilities(client)
    assert capabilities["sdk"] == true
    assert capabilities["telemetry"] == true

    acl_capable? = capabilities["acl_management"] == true

    assert_management_capability_response(SDK.acl_list_users(client), acl_capable?)
    assert_management_capability_response(SDK.acl_get_user(client, "default"), acl_capable?)

    case SDK.acl_set_user(client, username, ["on"]) do
      {:ok, _value} ->
        assert_docker_command_response(SDK.acl_get_user(client, username))
        assert_docker_command_response(SDK.acl_del_user(client, username))
        assert_docker_command_response(SDK.acl_save(client))

      {:error, reason} ->
        assert_error_message(reason, "unsupported")
    end

    namespace_capable? = capabilities["namespace_management"] == true
    assert_management_capability_response(SDK.list_namespaces(client), namespace_capable?)
    assert_management_capability_response(SDK.get_namespace(client, prefix), namespace_capable?)

    assert_management_capability_response(
      SDK.ensure_namespace(client, prefix, durability: :raft),
      namespace_capable?
    )

    assert_management_capability_response(
      SDK.delete_namespace(client, prefix),
      namespace_capable?
    )

    quota_capable? = capabilities["quota_management"] == true
    assert_management_capability_response(SDK.get_quota(client, prefix), quota_capable?)
    assert_management_capability_response(SDK.set_quota(client, prefix, keys: 10), quota_capable?)
    assert_management_capability_response(SDK.quota_usage(client, prefix), quota_capable?)

    assert {:ok, cluster_info} = SDK.cluster_info(client)
    assert is_map(cluster_info)

    assert {:ok, namespace_usage} = SDK.namespace_usage(client, prefix)
    assert namespace_usage["prefix"] == prefix

    assert {:ok, flow_query} = SDK.flow_query(client, tenant: "acme")
    assert is_list(flow_query)

    assert {:ok, flow_history} = SDK.flow_history(client, unique("flow-history"))
    assert is_list(flow_history)
  end

  test "topology-aware SDK Flow wrappers reach Docker and classify unsafe handlers" do
    client = start_sdk_client("flow-all")
    base = unique("sdk-flow-all")

    classified =
      flow_safe_invocation_cases(base)
      |> Keyword.keys()
      |> MapSet.new()
      |> MapSet.union(MapSet.new(flow_unsafe_invocation_functions()))

    assert classified ==
             MapSet.new(Map.keys(Flow.opcodes()))

    for {function, payload} <- flow_safe_invocation_cases(base) do
      assert_docker_command_response(apply(Flow, function, [client, payload]))
    end
  end

  test "KV helpers cover set, get, mset, mget, and delete", %{client: client} do
    prefix = unique("kv")
    key = "{#{prefix}}:one"
    other_key = "{#{prefix}}:two"

    assert :ok = FerricStore.set(client, key, "value")
    assert FerricStore.get(client, key) == "value"

    assert FerricStore.mset(client, %{key => "value-2", other_key => "other"}) in ["OK", :ok]

    assert FerricStore.mget(client, [key, other_key, "{#{prefix}}:missing"]) == [
             "value-2",
             "other",
             nil
           ]

    assert_integer_like(FerricStore.delete(client, [key, other_key], atomicity: :per_shard), 2)
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
    assert FerricStore.zscore(client, key, "a") == 1.0
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
    partition_key = unique("partition")

    ref =
      FerricStore.Flow.value_put(client, "large-value", partition_key: partition_key)

    assert is_binary(ref) or is_map(ref)

    assert FerricStore.Flow.create(client, id,
             type: type,
             partition_key: partition_key,
             payload: "payload",
             attributes: %{tenant: "acme"},
             value_refs: %{blob: extract_ref(ref)},
             now_ms: System.system_time(:millisecond)
           ) in ["OK", "QUEUED", "CREATED"]

    assert_value_mget(client, ref, "large-value")
    assert is_map(FerricStore.Flow.get(client, id, payload: true, partition_key: partition_key))
    assert is_list(FerricStore.Flow.list(client, type: type, state: "queued", count: 10))

    assert [{event_id, history_record} | _history] =
             FerricStore.Flow.history(client, id, partition_key: partition_key)

    assert is_binary(event_id)
    assert history_record["id"] == id
    assert history_record["event"] == "created"

    [job | _] = claim_one(client, type, "queued", worker, partition_key: partition_key)
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

    [processing_job | _] =
      claim_one(client, type, "processing", worker, partition_key: partition_key)

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

  test "flow FIFO state policy is opt-in and partition scoped", %{client: client} do
    suffix = unique("fifo-policy")
    type = "#{suffix}-type"
    partition_a = "#{suffix}:partition-a"
    partition_b = "#{suffix}:partition-b"
    partition_c = "#{suffix}:partition-c"
    first_a = "#{suffix}:z-first-a"
    second_a = "#{suffix}:a-second-a"
    first_b = "#{suffix}:z-first-b"
    ignored_c = "#{suffix}:z-ignored-c"

    policy =
      FerricStore.Flow.policy_set(client, type,
        states: %{
          "queued" => [mode: :fifo],
          "ready" => %{mode: :fifo}
        }
      )

    assert get_in(policy, ["states", "queued", "mode"]) in ["fifo", :fifo]
    assert get_in(policy, ["states", "ready", "mode"]) in ["fifo", :fifo]

    assert_okish(
      FerricStore.Flow.create(client, "#{suffix}:parallel-default",
        type: type,
        state: "not-fifo",
        priority: 1,
        payload: "parallel",
        now_ms: 1_000,
        run_at_ms: 2_000
      )
    )

    assert {:error, reason} =
             FerricStore.Flow.create(client, "#{suffix}:missing-partition",
               type: type,
               state: "queued",
               payload: "missing",
               now_ms: 1_000,
               run_at_ms: 2_000
             )

    assert_error_message(reason, "partition_key is required for fifo state")

    assert {:error, reason} =
             FerricStore.Flow.create(client, "#{suffix}:priority",
               type: type,
               state: "queued",
               partition_key: partition_a,
               priority: 1,
               payload: "priority",
               now_ms: 1_000,
               run_at_ms: 2_000
             )

    assert_error_message(reason, "priority is not supported for fifo state")

    for {id, partition, now_ms} <- [
          {first_a, partition_a, 1_000},
          {second_a, partition_a, 1_001},
          {first_b, partition_b, 1_002},
          {ignored_c, partition_c, 1_003}
        ] do
      assert_okish(
        FerricStore.Flow.create(client, id,
          type: type,
          state: "queued",
          partition_key: partition,
          payload: id,
          now_ms: now_ms,
          run_at_ms: 2_000
        )
      )
    end

    claimed =
      FerricStore.Flow.claim_due(client, type,
        state: "queued",
        partition_keys: [partition_a, partition_b],
        worker: "#{suffix}:worker",
        limit: 10,
        include_attributes: false,
        now_ms: 2_000
      )

    assert MapSet.new(Enum.map(claimed, & &1["id"])) == MapSet.new([first_a, first_b])
    assert Enum.all?(claimed, &(&1["partition_key"] in [partition_a, partition_b]))
    assert Enum.all?(claimed, &is_binary(&1["lease_token"]))
    assert Enum.all?(claimed, &is_integer(&1["fencing_token"]))

    assert [] =
             FerricStore.Flow.claim_due(client, type,
               state: "queued",
               partition_keys: [partition_a, partition_b],
               worker: "#{suffix}:worker",
               limit: 10,
               include_attributes: false,
               now_ms: 2_001
             )

    assert [%{"id" => ^ignored_c, "partition_key" => ^partition_c}] =
             FerricStore.Flow.claim_due(client, type,
               state: "queued",
               partition_keys: [partition_c],
               worker: "#{suffix}:worker",
               limit: 10,
               include_attributes: false,
               now_ms: 2_001
             )
  end

  test "flow transition into FIFO states carries partition key and rejects priority", %{
    client: client
  } do
    suffix = unique("fifo-transition")
    type = "#{suffix}-type"
    missing_type = "#{suffix}-missing-type"
    partition = "#{suffix}:partition"
    success_id = "#{suffix}:success"
    priority_id = "#{suffix}:priority"
    missing_partition_id = "#{suffix}:missing-partition-id"

    assert %{"states" => %{"ready" => %{"mode" => mode}}} =
             FerricStore.Flow.policy_set(client, type, states: %{"ready" => [mode: :fifo]})

    assert mode in ["fifo", :fifo]

    assert %{"states" => %{"ready" => %{"mode" => missing_mode}}} =
             FerricStore.Flow.policy_set(client, missing_type,
               states: %{"ready" => [mode: :fifo]}
             )

    assert missing_mode in ["fifo", :fifo]

    for id <- [success_id, priority_id] do
      assert_okish(
        FerricStore.Flow.create(client, id,
          type: type,
          state: "intake",
          partition_key: partition,
          payload: id,
          now_ms: 1_000,
          run_at_ms: 1_000
        )
      )
    end

    assert_okish(
      FerricStore.Flow.create(client, missing_partition_id,
        type: missing_type,
        state: "intake",
        payload: missing_partition_id,
        now_ms: 1_000,
        run_at_ms: 1_000
      )
    )

    [success_job] =
      FerricStore.Flow.claim_due(client, type,
        state: "intake",
        partition_key: partition,
        worker: "#{suffix}:worker",
        limit: 1,
        include_attributes: false,
        now_ms: 1_000
      )

    success_claimed_id = success_job["id"]

    assert_okish(
      FerricStore.Flow.transition(client, success_claimed_id,
        from_state: "running",
        to_state: "ready",
        partition_key: success_job["partition_key"],
        lease_token: success_job["lease_token"],
        fencing_token: success_job["fencing_token"],
        now_ms: 1_100,
        run_at_ms: 1_100
      )
    )

    [ready_job] =
      FerricStore.Flow.claim_due(client, type,
        state: "ready",
        partition_keys: [partition],
        worker: "#{suffix}:ready-worker",
        limit: 1,
        include_attributes: false,
        now_ms: 1_100
      )

    assert ready_job["id"] == success_claimed_id
    assert ready_job["partition_key"] == partition

    [priority_job] =
      FerricStore.Flow.claim_due(client, type,
        state: "intake",
        partition_key: partition,
        worker: "#{suffix}:worker",
        limit: 1,
        include_attributes: false,
        now_ms: 1_200
      )

    assert {:error, reason} =
             FerricStore.Flow.transition(client, priority_job["id"],
               from_state: "running",
               to_state: "ready",
               partition_key: priority_job["partition_key"],
               lease_token: priority_job["lease_token"],
               fencing_token: priority_job["fencing_token"],
               priority: 1,
               now_ms: 1_300,
               run_at_ms: 1_300
             )

    assert_error_message(reason, "priority is not supported for fifo state")

    [missing_partition_job] =
      FerricStore.Flow.claim_due(client, missing_type,
        state: "intake",
        worker: "#{suffix}:worker",
        limit: 1,
        include_attributes: false,
        now_ms: 1_400
      )

    assert {:error, reason} =
             FerricStore.Flow.transition(client, missing_partition_job["id"],
               from_state: "running",
               to_state: "ready",
               lease_token: missing_partition_job["lease_token"],
               fencing_token: missing_partition_job["fencing_token"],
               now_ms: 1_500,
               run_at_ms: 1_500
             )

    assert_error_message(reason, "partition_key is required for fifo state")
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

    case FerricStore.SDK.Flow.policy_set(client, %{
           type: type,
           indexed_state_meta: "version"
         }) do
      {:ok, policy} ->
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

      {:error, reason} ->
        assert_error_message(reason, "unknown flow option indexed_state_meta")
    end

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

  defp start_sdk_client(name) do
    {:ok, client} =
      SDK.start_link(
        url: @docker_url,
        client_name: "ferricstore-elixir-sdk-#{name}",
        endpoint_policy: :any
      )

    on_exit(fn -> SDK.close(client) end)
    client
  end

  defp same_slot_key(tag, suffix), do: "elixir-sdk:{#{tag}}:#{suffix}"

  defp admin_safe_invocation_cases(key) do
    [
      cluster_health: %{},
      cluster_stats: %{},
      cluster_keyslot: %{args: [key]},
      cluster_slots: %{},
      cluster_status: %{},
      cluster_role: %{},
      ferricstore_key_info: %{key: key},
      ferricstore_config: %{args: ["GET", "native-port"]},
      ferricstore_hotness: %{args: [key]},
      ferricstore_metrics: %{}
    ]
  end

  defp admin_unsafe_invocation_functions do
    [
      :cluster_join,
      :cluster_leave,
      :cluster_failover,
      :cluster_promote,
      :cluster_demote,
      :ferricstore_blobgc
    ]
  end

  defp flow_safe_invocation_cases(base) do
    type = "#{base}-type"
    id = "#{base}-id"
    parent_id = "#{base}-parent"
    root_id = "#{base}-root"
    correlation_id = "#{base}-correlation"
    scope = "#{base}-scope"
    now_ms = System.system_time(:millisecond)

    [
      create: %{
        id: id,
        type: type,
        state: "queued",
        partition_key: base,
        parent_flow_id: parent_id,
        root_flow_id: root_id,
        correlation_id: correlation_id,
        attributes: %{tenant: "acme"},
        now_ms: now_ms
      },
      get: %{id: id, partition_key: base},
      claim_due: %{type: type, state: "queued", worker: "#{base}-worker", limit: 1},
      complete: %{
        id: "#{base}-missing",
        partition_key: base,
        lease_token: "missing",
        fencing_token: 0
      },
      transition: %{
        id: "#{base}-missing",
        partition_key: base,
        from_state: "running",
        to_state: "done",
        lease_token: "missing",
        fencing_token: 0
      },
      retry: %{
        id: "#{base}-missing",
        partition_key: base,
        lease_token: "missing",
        fencing_token: 0,
        error: "retry"
      },
      fail: %{
        id: "#{base}-missing",
        partition_key: base,
        lease_token: "missing",
        fencing_token: 0,
        error: "fail"
      },
      cancel: %{
        id: "#{base}-missing",
        partition_key: base,
        fencing_token: 0,
        reason: "cancel"
      },
      extend_lease: %{
        id: "#{base}-missing",
        partition_key: base,
        lease_token: "missing",
        fencing_token: 0,
        ttl_ms: 1_000
      },
      history: %{id: id},
      value_put: %{value: "value"},
      value_mget: %{refs: []},
      signal: %{id: id, signal: "noop", payload: %{}},
      list: %{type: type, state: "queued", count: 10},
      create_many: %{
        type: "#{type}-many",
        independent: true,
        return: "OK_ON_SUCCESS",
        items: [["#{base}-many-1", ""], ["#{base}-many-2", ""]]
      },
      complete_many: %{items: []},
      transition_many: %{from_state: "running", to_state: "done", items: []},
      retry_many: %{items: []},
      fail_many: %{items: []},
      cancel_many: %{items: []},
      reclaim: %{type: type, state: "running", worker: "#{base}-reclaimer", limit: 1},
      rewind: %{id: "#{base}-missing", partition_key: base, to_state: "queued"},
      terminals: %{type: type},
      failures: %{type: type},
      by_parent: %{parent_id: parent_id},
      by_root: %{root_id: root_id},
      by_correlation: %{correlation_id: correlation_id},
      info: %{type: type},
      stuck: %{type: type, now_ms: now_ms},
      policy_set: %{type: type, indexed_state_meta: "version"},
      policy_get: %{type: type},
      stats: %{type: type},
      attributes: %{type: type},
      attribute_values: %{type: type, attribute: "tenant"},
      search: %{type: type, attributes: %{tenant: "acme"}, count: 10, terminal_only: true},
      governance_ledger: %{id: id},
      approval_get: %{id: "#{base}-approval"},
      circuit_get: %{scope: scope},
      budget_get: %{scope: scope},
      limit_get: %{scope: scope},
      approval_list: %{flow_id: id},
      governance_overview: %{scope: scope},
      budget_list: %{scope: scope},
      limit_list: %{scope: scope}
    ]
  end

  defp flow_unsafe_invocation_functions do
    [
      :spawn_children,
      :retention_cleanup,
      :step_continue,
      :start_and_claim,
      :run_steps_many,
      :schedule_create,
      :schedule_get,
      :schedule_delete,
      :schedule_fire_due,
      :schedule_list,
      :schedule_fire,
      :schedule_pause,
      :schedule_resume,
      :effect_reserve,
      :effect_confirm,
      :effect_fail,
      :effect_compensate,
      :effect_get,
      :approval_request,
      :approval_approve,
      :approval_reject,
      :circuit_open,
      :circuit_close,
      :budget_reserve,
      :budget_commit,
      :budget_release,
      :limit_lease,
      :limit_spend,
      :limit_release
    ]
  end

  defp assert_sdk_opcode_table_matches(options) do
    advertised =
      Map.new(options["opcodes"], fn %{"name" => name, "opcode" => opcode} ->
        {name, opcode}
      end)

    known =
      Opcodes.all()
      |> Enum.map(fn {_atom, opcode} -> {Opcodes.name(opcode), opcode} end)
      |> Map.new()

    advertised_names = advertised |> Map.keys() |> MapSet.new()
    known_names = known |> Map.keys() |> MapSet.new()

    assert MapSet.difference(advertised_names, known_names) == MapSet.new()

    mismatched =
      advertised
      |> Enum.reject(fn {name, opcode} -> known[name] == opcode end)
      |> Enum.map(fn {name, opcode} -> {name, opcode, known[name]} end)

    assert mismatched == []
    assert Opcodes.fetch!("GOAWAY") == 0x000A
    assert Opcodes.fetch!("EVENT") == 0x0010
  end

  defp assert_management_capability_response({:ok, _value}, _capable?), do: :ok

  defp assert_management_capability_response({:error, reason}, false) do
    assert_error_message(reason, "unsupported")
  end

  defp assert_management_capability_response({:error, reason}, true) do
    flunk("expected management command to be supported, got #{inspect(reason)}")
  end

  defp assert_docker_command_response({:ok, _value}), do: :ok

  defp assert_docker_command_response({:error, reason}) do
    message = error_reason_message(reason)
    downcased = String.downcase(message)

    refute String.contains?(downcased, "unknown command")
    refute String.contains?(downcased, "unknown opcode")
    assert byte_size(message) > 0
  end

  defp assert_docker_command_response(other) do
    flunk("expected Docker SDK command response, got #{inspect(other)}")
  end

  defp assert_error_message(reason, expected) do
    assert reason |> error_reason_message() |> String.downcase() =~ String.downcase(expected)
  end

  defp error_reason_message({_, %{"message" => message}}) when is_binary(message), do: message
  defp error_reason_message(%{"message" => message}) when is_binary(message), do: message
  defp error_reason_message(%{message: message}) when is_binary(message), do: message
  defp error_reason_message(reason), do: inspect(reason)

  defp extract_ref(%{"ref" => ref}), do: ref
  defp extract_ref(%{ref: ref}), do: ref
  defp extract_ref(ref) when is_binary(ref), do: ref

  defp unique(prefix) do
    "elixir-sdk-#{prefix}-#{System.system_time(:nanosecond)}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp claim_one(client, type, state, worker, opts \\ []) do
    opts = Keyword.merge([state: state, worker: worker, limit: 1], opts)
    jobs = FerricStore.Flow.claim_due(client, type, opts)

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
