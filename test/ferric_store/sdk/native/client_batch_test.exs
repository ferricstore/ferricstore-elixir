defmodule FerricStore.SDK.Native.ClientBatchTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Native.Topology
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "a slow multi-key request does not block unrelated routed calls" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> {:reply_after, 300, ["batch-value"]}
      %{opcode: 0x0101} -> "get-value"
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])
    on_exit(fn -> SDK.close(client) end)

    batch = Task.async(fn -> SDK.mget(client, ["batch-key"], timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500

    started = System.monotonic_time(:millisecond)
    assert {:ok, "get-value"} = SDK.get(client, "fast-key", timeout: 1_000)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 150
    assert Task.await(batch, 1_000) == {:ok, ["batch-value"]}
  end

  test "batch wire groups do not consume logical admission twice" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> {:reply_after, 200, ["batch-value"]}
      %{opcode: 0x0101} -> "get-value"
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_pending_requests: 2
      )

    on_exit(fn -> SDK.close(client) end)
    batch = Task.async(fn -> SDK.mget(client, ["batch-key"], timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500

    assert {:ok, "get-value"} = SDK.get(client, "independent-key", timeout: 500)
    assert Task.await(batch, 1_000) == {:ok, ["batch-value"]}
  end

  test "batch connection preflight reserves capacity for groups sharing an endpoint" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        port = Agent.get(port_holder, & &1)
        two_shard_topology(port, port)

      %{opcode: 0x000C} ->
        %{
          "protocol" => "ferricstore-native",
          "capabilities" => %{
            "flow_control" => %{
              "enforced" => true,
              "max_inflight_per_connection" => 1,
              "max_inflight_per_lane" => 1
            }
          }
        }

      %{opcode: 0x0104, payload: %{"keys" => [key]}} ->
        {:reply_after, 100, ["value:#{key}"]}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        connections_per_endpoint: 2
      )

    on_exit(fn -> SDK.close(client) end)
    keys = [key_in_slots(0..511), key_in_slots(512..1023)]
    expected = Enum.map(keys, &"value:#{&1}")

    assert {:ok, ^expected} = SDK.mget(client, keys, timeout: 1_000, max_group_concurrency: 2)

    assert NativeServer.connection_count(server) == 2
  end

  test "batch groups above total session capacity wait instead of partially failing" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        port = Agent.get(port_holder, & &1)
        three_shard_topology([port, port, port])

      %{opcode: 0x000C} ->
        %{
          "protocol" => "ferricstore-native",
          "capabilities" => %{
            "flow_control" => %{
              "enforced" => true,
              "max_inflight_per_connection" => 1,
              "max_inflight_per_lane" => 1
            }
          }
        }

      %{opcode: 0x0104, payload: %{"keys" => [key]}} ->
        {:reply_after, 60, ["value:#{key}"]}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(seeds: [{"127.0.0.1", port}], connections_per_endpoint: 2)

    on_exit(fn -> SDK.close(client) end)

    keys = [
      key_in_slots(0..340),
      key_in_slots(341..681),
      key_in_slots(682..1023)
    ]

    expected = Enum.map(keys, &"value:#{&1}")

    assert {:ok, ^expected} =
             SDK.mget(client, keys, timeout: 1_000, max_group_concurrency: 3)

    assert NativeServer.connection_count(server) == 2
  end

  test "batch preflight accounts for scalar requests already occupying the routed lane" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x000C} ->
        %{
          "protocol" => "ferricstore-native",
          "capabilities" => %{
            "flow_control" => %{
              "enforced" => true,
              "max_inflight_per_connection" => 2,
              "max_inflight_per_lane" => 1
            }
          }
        }

      %{opcode: 0x0101} ->
        {:reply_after, 200, "scalar-value"}

      %{opcode: 0x0104} ->
        ["batch-value"]

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(seeds: [{"127.0.0.1", port}], connections_per_endpoint: 2)

    on_exit(fn -> SDK.close(client) end)
    scalar = Task.async(fn -> SDK.get(client, "scalar-key", timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x0101}}, 200

    assert {:ok, ["batch-value"]} = SDK.mget(client, ["batch-key"], timeout: 1_000)
    assert NativeServer.connection_count(server) == 2
    assert Task.await(scalar, 1_000) == {:ok, "scalar-value"}
  end

  test "user batch callbacks do not run in the client coordinator" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    owner = self()

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> ["batch-value"]
      %{opcode: 0x0003} -> "pong"
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: owner, response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])
    on_exit(fn -> SDK.close(client) end)

    batch =
      Task.async(fn ->
        SDK.request_by_items(
          client,
          0x0104,
          ["slow-key"],
          fn key ->
            send(owner, {:batch_callback_started, self()})
            Process.sleep(250)
            key
          end,
          fn keys -> %{"keys" => keys} end,
          timeout: 1_000
        )
      end)

    assert_receive {:batch_callback_started, callback_process}, 200

    [coordinated_batch] =
      ClientRuntime.state(client)
      |> Map.fetch!(:batch_scheduler)
      |> Map.fetch!(:batches)
      |> Map.values()

    assert coordinated_batch.preparer.pid == callback_process

    started = System.monotonic_time(:millisecond)
    assert {:ok, "pong"} = SDK.ping(client, "ping", timeout: 500)
    assert System.monotonic_time(:millisecond) - started < 120
    assert {:ok, [%{value: ["batch-value"]}]} = Task.await(batch, 1_000)
  end

  test "trusted KV batches do not spawn a per-request preparation worker" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104, payload: %{"keys" => keys}} -> keys
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])
    on_exit(fn -> SDK.close(client) end)

    operation_supervisor = ClientRuntime.state(client).operation_supervisor
    :ok = :sys.suspend(operation_supervisor)

    try do
      assert {:ok, ["trusted-key"]} =
               SDK.mget(client, ["trusted-key"], timeout: 1_000, call_timeout: 500)
    after
      :ok = :sys.resume(operation_supervisor)
    end
  end

  test "one batch cannot bypass admission with an excessive item count" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_batch_items: 2
      )

    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    assert {:error, {:batch_too_large, %{items: 3, limit: 2}}} =
             SDK.mget(client, ["one", "two", "three"])

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 100
    assert ClientRuntime.state(client).batch_scheduler.batches == %{}
  end

  test "public native requests reject private batch cardinality metadata" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_batch_items: 1
      )

    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()
    opcode = FerricStore.Protocol.opcode(:mget)

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_request_option, :__batch_item_count__, {^opcode, 1}}
            }} =
             FerricStore.Client.native(
               client,
               opcode,
               %{"keys" => ["one", "two"]},
               __batch_item_count__: {opcode, 1}
             )

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 50
  end

  test "public requests reject internal deadline metadata" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    assert {:error,
            {:invalid_kv_input,
             %{
               operation: :get,
               field: :options,
               reason: :unsupported_options,
               options: [:__client_deadline__]
             }}} = SDK.get(client, "expired", timeout: 0, __client_deadline__: :infinity)

    refute_receive {:native_server_request, %{opcode: 0x0101}}, 50
  end

  test "high-level multi-shard writes require explicit partial atomicity" do
    {:ok, second_server} = NativeServer.start_link(owner: self())
    second_port = NativeServer.port(second_server)
    {:ok, seed_port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        two_shard_topology(Agent.get(seed_port_holder, & &1), second_port)

      %{opcode: 0x0103} ->
        1

      _request ->
        "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    seed_port = NativeServer.port(seed_server)
    Agent.update(seed_port_holder, fn _ -> seed_port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", seed_port}])
    on_exit(fn -> SDK.close(client) end)

    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)
    flush_native_server_messages()

    assert {:error,
            %FerricStore.Error{
              raw: {:multi_slot_write_requires_explicit_policy, :mset}
            }} = FerricStore.mset(client, [{first_key, "one"}, {second_key, "two"}])

    assert {:error,
            %FerricStore.Error{
              raw: {:multi_shard_write_requires_explicit_policy, :del}
            }} = FerricStore.delete(client, [first_key, second_key])

    refute_receive {:native_server_request, %{opcode: 0x0103}}, 50
    refute_receive {:native_server_request, %{opcode: 0x0105}}, 10

    assert :ok =
             FerricStore.mset(client, [{first_key, "one"}, {second_key, "two"}],
               atomicity: :per_slot
             )
  end

  test "mset rejects malformed pairs without leaking callback failures or reaching the wire" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    assert {:error, {:invalid_mset_pair, :invalid}} =
             SDK.mset(client, [{"valid", "value"}, :invalid])

    refute_receive {:native_server_request, %{opcode: 0x0105}}, 50
  end

  test "empty mset is a successful no-op under the current native contract" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    assert {:ok, :ok} = SDK.mset(client, [])
    assert {:ok, :ok} = SDK.mset(client, %{})
    refute_receive {:native_server_request, %{opcode: 0x0105}}, 50
  end

  test "the configured batch limit covers typed and compact Flow batches" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_batch_items: 2
      )

    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    for timeout <- [500, :infinity] do
      assert {:error,
              %FerricStore.Error{
                raw: {:batch_too_large, %{items: 3, limit: 2}}
              }} =
               FerricStore.Flow.create_many(client, ["one", "two", "three"],
                 type: "email",
                 timeout: timeout
               )
    end

    jobs = [
      {"one", "lease-1", 1},
      {"two", "lease-2", 2},
      {"three", "lease-3", 3}
    ]

    assert {:error,
            %FerricStore.Error{
              raw: {:batch_too_large, %{items: 3, limit: 2}}
            }} = FerricStore.Flow.complete_many(client, jobs)

    assert {:error, {:batch_too_large, %{items: 3, limit: 2}}} =
             FerricStore.SDK.Flow.transition_many(client, %{
               from_state: "queued",
               to_state: "running",
               items: [["one"], ["two"], ["three"]]
             })

    refute_receive {:native_server_request, %{opcode: 0x020F}}, 100
    refute_receive {:native_server_request, %{opcode: 0x0210}}, 10
    refute_receive {:native_server_request, %{opcode: 0x0211}}, 10
  end

  test "the configured batch limit covers every typed collection command" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_batch_items: 2
      )

    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    calls = [
      fn -> SDK.hset(client, "hash", %{"a" => "1", "b" => "2", "c" => "3"}) end,
      fn -> SDK.hmget(client, "hash", ["a", "b", "c"]) end,
      fn -> SDK.lpush(client, "list", ["a", "b", "c"]) end,
      fn -> SDK.rpush(client, "list", ["a", "b", "c"]) end,
      fn -> SDK.sadd(client, "set", ["a", "b", "c"]) end,
      fn -> SDK.srem(client, "set", ["a", "b", "c"]) end,
      fn -> SDK.zadd(client, "sorted", [{1, "a"}, {2, "b"}, {3, "c"}]) end,
      fn -> SDK.zrem(client, "sorted", ["a", "b", "c"]) end
    ]

    Enum.each(calls, fn call ->
      assert {:error, {:batch_too_large, %{items: 3, limit: 2}}} = call.()
    end)

    for opcode <- [0x0110, 0x0112, 0x0120, 0x0121, 0x0130, 0x0131, 0x0140, 0x0141] do
      refute_receive {:native_server_request, %{opcode: ^opcode}}, 10
    end
  end

  test "absolute batch admission rejects huge lists before entering the coordinator" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_batch_items: 10
      )

    on_exit(fn -> SDK.close(client) end)
    :ok = ClientRuntime.suspend(client)
    items = List.duplicate("too-large", 1_000_000)

    request =
      Task.async(fn ->
        try do
          SDK.mget(client, items, call_timeout: 50)
        catch
          :exit, {:timeout, _call} -> :entered_coordinator
        end
      end)

    try do
      assert Task.await(request, 250) ==
               {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}}
    after
      :ok = ClientRuntime.resume(client)
      Task.shutdown(request, :brutal_kill)
    end
  end

  test "oversized KV groups are rejected before payload allocation or coordinator submission" do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_request_bytes: 256
      )

    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()
    :ok = ClientRuntime.suspend(client)

    request =
      Task.async(fn ->
        try do
          SDK.mset(client, [{"oversized-key", String.duplicate("x", 1_024)}],
            atomicity: :per_slot,
            call_timeout: 50
          )
        catch
          :exit, {:timeout, _call} -> :entered_coordinator
        end
      end)

    try do
      assert Task.await(request, 250) == {:error, :request_too_large}
      refute_receive {:native_server_request, %{opcode: 0x0105}}, 50
    after
      :ok = ClientRuntime.resume(client)
      Task.shutdown(request, :brutal_kill)
    end
  end

  test "partial multi-shard writes identify every successful and failed input group" do
    {:ok, failed_server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0105} -> {:reply, "rejected", status: 6}
          _request -> "OK"
        end
      )

    failed_port = NativeServer.port(failed_server)
    {:ok, seed_port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        two_shard_topology(Agent.get(seed_port_holder, & &1), failed_port)

      %{opcode: 0x000C} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    seed_port = NativeServer.port(seed_server)
    Agent.update(seed_port_holder, fn _ -> seed_port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", seed_port}])

    on_exit(fn -> SDK.close(client) end)

    success_key = key_in_slots(0..511)
    failed_key = key_in_slots(512..1023)

    result =
      SDK.mset(client, [{success_key, "one"}, {failed_key, "two"}], atomicity: :per_slot)

    assert {:error,
            {:partial_group_failure,
             %{
               successes: [%{indexes: [0], value: "OK"}],
               failures: [
                 %{
                   indexes: [1],
                   route: %{shard: 1, lane_id: 2},
                   reason: {:bad_request, "rejected"}
                 }
               ]
             }}} = result

    refute inspect(result) =~ "\"one\""
    refute inspect(result) =~ "\"two\""
  end

  test "partial connection setup retains established connections without orphaning children" do
    {:ok, data_server} = NativeServer.start_link(owner: self())
    data_port = NativeServer.port(data_server)
    unavailable_port = unused_port()

    response_fun = fn
      %{opcode: 0x0007} -> two_shard_topology(data_port, unavailable_port)
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    seed_port = NativeServer.port(seed_server)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", seed_port}])
    on_exit(fn -> SDK.close(client) end)

    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)

    assert {:error, {:retry_failed, {:connect_failed, _}, {:connect_failed, _}}} =
             SDK.mset(client, [{first_key, "one"}, {second_key, "two"}], atomicity: :per_slot)

    state = ClientRuntime.state(client)
    tracked = state.connection_pool.connections |> Map.values() |> MapSet.new()

    supervised =
      state.connection_supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.map(fn {_id, pid, _type, _modules} -> pid end)
      |> MapSet.new()

    assert tracked == supervised
    assert MapSet.size(tracked) == 2
    assert NativeServer.connection_count(data_server) == 1
  end

  test "a read batch retries when every shard group fails before execution" do
    {:ok, first_data} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0104} -> ["first"]
          _request -> "OK"
        end
      )

    {:ok, second_data} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0104} -> ["second"]
          _request -> "OK"
        end
      )

    live_ports = [NativeServer.port(first_data), NativeServer.port(second_data)]
    dead_ports = [unused_port(), unused_port()]
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)

    seed_response = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1, &1 + 1})
        [first_port, second_port] = if request == 0, do: dead_ports, else: live_ports
        two_shard_topology(first_port, second_port)

      %{opcode: 0x000C} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: seed_response)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed_server)}],
        endpoint_policy: :any
      )

    on_exit(fn ->
      SDK.close(client)

      Enum.each([seed_server, first_data, second_data], fn server ->
        if Process.alive?(server), do: GenServer.stop(server, :normal)
      end)
    end)

    keys = [key_in_slots(0..511), key_in_slots(512..1023)]

    assert {:ok, ["first", "second"]} = SDK.mget(client, keys, timeout: 1_000)
    assert Agent.get(shard_requests, & &1) >= 2
  end

  test "one absolute batch deadline prevents later queued groups from reaching the wire" do
    {:ok, second_server} = NativeServer.start_link(owner: self())
    second_port = NativeServer.port(second_server)
    {:ok, seed_port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        two_shard_topology(Agent.get(seed_port_holder, & &1), second_port)

      %{opcode: 0x000C} ->
        %{"protocol" => "ferricstore-native"}

      %{opcode: 0x0104} ->
        {:reply_after, 150, ["first"]}

      _request ->
        "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    seed_port = NativeServer.port(seed_server)
    Agent.update(seed_port_holder, fn _ -> seed_port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", seed_port}])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)

    request =
      Task.async(fn ->
        try do
          SDK.mget(client, [first_key, second_key],
            timeout: 1_000,
            call_timeout: 80,
            max_group_concurrency: 1
          )
        catch
          :exit, {:timeout, _call} -> :caller_timed_out
        end
      end)

    assert_receive {:native_server_request, %{opcode: 0x0104}}, 200
    result = Task.await(request, 200)

    second_group_sent? =
      receive do
        {:native_server_request, %{opcode: 0x0104}} -> true
      after
        200 -> false
      end

    assert {result, second_group_sent?} == {{:error, :timeout}, false}
  end

  test "a slow batch connection handshake does not block existing control traffic" do
    data_response = fn
      %{opcode: 0x000C} -> {:reply_after, 300, %{"protocol" => "ferricstore-native"}}
      %{opcode: 0x0104} -> ["batch"]
      _request -> "OK"
    end

    {:ok, data_server} = NativeServer.start_link(owner: self(), response_fun: data_response)
    data_port = NativeServer.port(data_server)

    seed_response = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(data_port, node: "data")
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: self(), response_fun: seed_response)
    seed_endpoint = %{host: "127.0.0.1", native_port: NativeServer.port(seed_server)}
    {:ok, client} = SDK.start_link(seeds: [seed_endpoint])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    batch = Task.async(fn -> SDK.mget(client, ["slow-batch"], timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x000C}}, 200
    started = System.monotonic_time(:millisecond)

    assert {:ok, "OK"} = SDK.ping(client, "responsive", endpoint: seed_endpoint)
    assert System.monotonic_time(:millisecond) - started < 150
    assert Task.await(batch, 1_500) == {:ok, ["batch"]}
  end

  test "batch connection setup obeys the group concurrency limit" do
    owner = self()

    data_servers =
      Enum.map(1..3, fn index ->
        response_fun = fn
          %{opcode: 0x000C} ->
            send(owner, {:data_hello, index})
            {:reply_after, 100, %{"protocol" => "ferricstore-native"}}

          %{opcode: 0x0104} ->
            ["value-#{index}"]

          _request ->
            "OK"
        end

        {:ok, server} = NativeServer.start_link(owner: owner, response_fun: response_fun)
        server
      end)

    ports = Enum.map(data_servers, &NativeServer.port/1)

    seed_response = fn
      %{opcode: 0x0007} -> three_shard_topology(ports)
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: owner, response_fun: seed_response)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(seed_server)}])
    on_exit(fn -> SDK.close(client) end)
    flush_native_server_messages()

    keys = [key_in_slots(0..340), key_in_slots(341..681), key_in_slots(682..1023)]

    batch =
      Task.async(fn ->
        SDK.mget(client, keys, timeout: 1_000, max_group_concurrency: 1)
      end)

    assert_receive {:data_hello, _first}, 200
    refute_receive {:data_hello, _second}, 50
    assert {:ok, _values} = Task.await(batch, 1_500)
  end

  test "batch connection setup queues behind the global connecting limit" do
    owner = self()

    data_servers =
      Enum.map(1..2, fn index ->
        response_fun = fn
          %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
          %{opcode: 0x0104} -> ["value-#{index}"]
          _request -> "OK"
        end

        {:ok, server} = NativeServer.start_link(owner: owner, response_fun: response_fun)
        server
      end)

    ports = Enum.map(data_servers, &NativeServer.port/1)

    seed_response = fn
      %{opcode: 0x0007} -> two_shard_topology(Enum.at(ports, 0), Enum.at(ports, 1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: owner, response_fun: seed_response)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed_server)}],
        endpoint_policy: :any,
        max_connecting: 1
      )

    on_exit(fn -> SDK.close(client) end)
    keys = [key_in_slots(0..511), key_in_slots(512..1023)]

    assert {:ok, ["value-1", "value-2"]} =
             SDK.mget(client, keys, timeout: 1_000, max_group_concurrency: 2)
  end

  test "a batch resumes after an unrelated connection attempt releases the global slot" do
    owner = self()

    first_response = fn
      %{opcode: 0x000C} ->
        send(owner, :first_connection_started)
        {:reply_after, 100, %{"protocol" => "ferricstore-native"}}

      %{opcode: 0x0104} ->
        ["first"]

      _request ->
        "OK"
    end

    second_response = fn
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> ["second"]
      _request -> "OK"
    end

    {:ok, first_server} = NativeServer.start_link(owner: owner, response_fun: first_response)
    {:ok, second_server} = NativeServer.start_link(owner: owner, response_fun: second_response)

    seed_response = fn
      %{opcode: 0x0007} ->
        two_shard_topology(
          NativeServer.port(first_server),
          NativeServer.port(second_server)
        )

      %{opcode: 0x000C} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, seed_server} = NativeServer.start_link(owner: owner, response_fun: seed_response)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed_server)}],
        endpoint_policy: :any,
        max_connecting: 1
      )

    on_exit(fn -> SDK.close(client) end)
    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)
    first = Task.async(fn -> SDK.mget(client, [first_key], timeout: 1_000) end)
    assert_receive :first_connection_started, 200
    second = Task.async(fn -> SDK.mget(client, [second_key], timeout: 1_000) end)

    assert Task.await(first, 1_500) == {:ok, ["first"]}
    assert Task.await(second, 1_500) == {:ok, ["second"]}
  end

  test "caller cancellation releases an infinite batch while retaining sent transport credit" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> :noreply
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])
    on_exit(fn -> SDK.close(client) end)

    caller =
      spawn(fn ->
        SDK.mget(client, ["abandoned"], timeout: :infinity, call_timeout: :infinity)
      end)

    assert_receive {:native_server_request, %{opcode: 0x0104}}, 200

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).batch_scheduler.batches) == 1
    end)

    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      pending =
        Enum.flat_map(state.connection_pool.connections, fn {_key, conn} ->
          conn |> :sys.get_state() |> Map.fetch!(:pending) |> Map.values()
        end)

      state.batch_scheduler.batches == %{} and state.request_registry.requests == %{} and
        match?([%{phase: :discarding, target: :discard, flow_controlled?: true}], pending)
    end)
  end

  test "a batch retry refresh is cancelled when its caller terminates" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})

        if request == 1,
          do: NativeServer.topology_payload(Agent.get(port_holder, & &1)),
          else: :noreply

      %{opcode: 0x000C} ->
        %{"protocol" => "ferricstore-native"}

      %{opcode: 0x0104} ->
        {:reply, "moved", status: 5}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        topology_refresh_timeout: 1_000
      )

    caller =
      spawn(fn ->
        SDK.mget(client, ["abandoned-batch"],
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_eventually(fn ->
      match?(
        %{waiters: [{:batch_retry, _batch_id}]},
        ClientRuntime.state(client).topology_manager.refresh_operation
      )
    end)

    refresher = ClientRuntime.state(client).topology_manager.refresh_operation.refresher
    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      state.batch_scheduler.batches == %{} and state.request_registry.requests == %{} and
        is_nil(state.topology_manager.refresh_operation)
    end)

    refute Process.alive?(refresher)
    assert Process.alive?(client)
  end

  test "batch caller cancellation scales near-linearly" do
    batch_cancel_reductions(12)
    small = batch_cancel_reductions(40)
    large = batch_cancel_reductions(80)

    assert large < small * 3,
           "expected near-linear cancellation reductions, got #{small} for 40 and #{large} for 80"
  end

  @tag capture_log: true
  test "batch preparation workers terminate when their owning client dies" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    Process.unlink(ClientRuntime.state(client).runtime_supervisor)
    test_pid = self()

    caller =
      spawn(fn ->
        SDK.request_by_items(
          client,
          0x0104,
          ["blocked"],
          fn key ->
            send(test_pid, {:batch_preparer_started, self()})

            receive do
              :release -> key
            end
          end,
          fn keys -> %{"keys" => keys} end,
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_receive {:batch_preparer_started, worker}, 250
    monitor = Process.monitor(worker)

    try do
      Process.exit(client, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^worker, _reason}, 250
    after
      if Process.alive?(worker), do: Process.exit(worker, :kill)
      if Process.alive?(caller), do: Process.exit(caller, :kill)
    end
  end

  defp batch_cancel_reductions(count) do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0104} -> :noreply
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_pending_requests: count * 3
      )

    callers =
      Enum.map(1..count, fn index ->
        spawn(fn ->
          SDK.mget(client, ["cancel-batch-#{index}"],
            timeout: :infinity,
            call_timeout: :infinity
          )
        end)
      end)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      map_size(state.batch_scheduler.batches) == count and
        map_size(state.request_registry.requests) == count
    end)

    {:reductions, before_reductions} = Process.info(client, :reductions)
    Enum.each(callers, &Process.exit(&1, :kill))

    assert_eventually(fn ->
      state = ClientRuntime.state(client)
      state.batch_scheduler.batches == %{} and state.request_registry.requests == %{}
    end)

    {:reductions, after_reductions} = Process.info(client, :reductions)
    SDK.close(client)
    if Process.alive?(server), do: GenServer.stop(server, :normal)
    after_reductions - before_reductions
  end

  defp key_in_slots(range) do
    Enum.find_value(1..10_000, fn index ->
      key = "batch-key-#{index}"
      if Topology.slot_for_key(key) in range, do: key
    end)
  end

  defp two_shard_topology(first_port, second_port) do
    %{
      "route_epoch" => 1,
      "shard_count" => 2,
      "ranges" => [
        range(0, 511, 0, 1, first_port),
        range(512, 1023, 1, 2, second_port)
      ]
    }
  end

  defp three_shard_topology([first_port, second_port, third_port]) do
    %{
      "route_epoch" => 1,
      "shard_count" => 3,
      "ranges" => [
        range(0, 340, 0, 1, first_port),
        range(341, 681, 1, 2, second_port),
        range(682, 1023, 2, 3, third_port)
      ]
    }
  end

  defp range(first, last, shard, lane_id, port) do
    %{
      "first_slot" => first,
      "last_slot" => last,
      "shard" => shard,
      "lane_id" => lane_id,
      "endpoint" => %{
        "node" => "node-#{shard}",
        "host" => "127.0.0.1",
        "native_port" => port
      }
    }
  end

  defp unused_port do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, {_address, port}} = :inet.sockname(listener)
    :ok = :gen_tcp.close(listener)
    port
  end

  defp flush_native_server_messages do
    receive do
      {:native_server_connected, _handler} -> flush_native_server_messages()
      {:native_server_request, _request} -> flush_native_server_messages()
      {:native_server_disconnected, _handler, _reason} -> flush_native_server_messages()
    after
      10 -> :ok
    end
  end

  defp assert_eventually(fun, attempts \\ 80)

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
