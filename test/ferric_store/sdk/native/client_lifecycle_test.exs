defmodule FerricStore.SDK.Native.ClientLifecycleTest do
  use ExUnit.Case, async: true

  alias FerricStore.{ClientIdentity, SDK}

  alias FerricStore.SDK.Native.{
    Client,
    ClientSupervisor,
    Connection,
    ConnectionPool,
    ConnectionStarter,
    Topology
  }

  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "each client owns an explicit supervised runtime tree" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    assert {:ok, coordinator} = ClientSupervisor.coordinator(client)
    state = :sys.get_state(coordinator)

    assert state.runtime_supervisor == client

    children =
      state.runtime_supervisor
      |> Supervisor.which_children()
      |> Map.new(fn {id, pid, _type, _modules} -> {id, pid} end)

    assert children.coordinator == coordinator
    assert children.connections == state.connection_supervisor
    assert children.operations == state.operation_supervisor
    assert children.event_fanout == state.event_fanout.pid
    assert is_pid(children.owner_guard)

    runtime_pids = Map.values(children) ++ [client]
    monitors = Map.new(runtime_pids, &{Process.monitor(&1), &1})

    assert :ok = SDK.close(client)

    Enum.each(monitors, fn {monitor, pid} ->
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 500
    end)

    stop_server(server)
  end

  test "the coordinator exclusively owns protected endpoint snapshots" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

    on_exit(fn ->
      if Process.alive?(client), do: SDK.close(client)
      stop_server(server)
    end)

    assert {:ok, endpoint} = ClientIdentity.endpoint(client)
    assert {:ok, coordinator} = ClientSupervisor.coordinator(client)
    assert :ets.info(endpoint, :protection) == :protected
    assert :ets.info(endpoint, :owner) == coordinator

    assert_raise ArgumentError, fn ->
      :ets.insert(endpoint, {:coordinator, self()})
    end

    assert {:ok, _version, %Topology{}} = ClientSupervisor.topology_snapshot(client)
    assert {:ok, "OK"} = SDK.ping(client)
  end

  test "the public client child spec is the supervised runtime" do
    assert %{start: {Client, :start_link, [[]]}, type: :supervisor} = Client.child_spec([])
  end

  test "an OTP parent supervises the public client runtime root" do
    {:ok, server} = NativeServer.start_link(owner: self())
    opts = [seeds: [{"127.0.0.1", NativeServer.port(server)}]]
    {:ok, parent} = Supervisor.start_link([{Client, opts}], strategy: :one_for_one)

    assert [{Client, client, :supervisor, [Client]}] = Supervisor.which_children(parent)
    assert {:ok, coordinator} = ClientSupervisor.coordinator(client)
    assert {:ok, "OK"} = SDK.ping(client)

    client_monitor = Process.monitor(client)
    coordinator_monitor = Process.monitor(coordinator)

    assert :ok = Supervisor.stop(parent)
    assert_receive {:DOWN, ^client_monitor, :process, ^client, :shutdown}, 500
    assert_receive {:DOWN, ^coordinator_monitor, :process, ^coordinator, :shutdown}, 500

    stop_server(server)
  end

  test "request resolution does not serialize through the supervisor mailbox" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    :ok = :sys.suspend(client)

    try do
      assert {:ok, "OK"} = SDK.ping(client, "mailbox-independent", timeout: 500)
    after
      :ok = :sys.resume(client)
      :ok = SDK.close(client)
      stop_server(server)
    end
  end

  test "topology refresh has a finite caller-controlled deadline" do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    {:ok, coordinator} = ClientSupervisor.coordinator(client)
    :ok = :sys.suspend(coordinator)

    try do
      assert {:error, {:invalid_timeout, -1}} = SDK.refresh_topology(client, -1)

      started = System.monotonic_time(:millisecond)
      assert {:error, :timeout} = SDK.refresh_topology(client, 10)
      assert System.monotonic_time(:millisecond) - started < 100
    after
      :ok = :sys.resume(coordinator)
      :ok = SDK.close(client)
      stop_server(server)
    end
  end

  test "the runtime shuts down when its starting process exits normally" do
    {:ok, server} = NativeServer.start_link(owner: self())
    parent = self()

    owner =
      spawn(fn ->
        {:ok, client} =
          SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

        {:ok, coordinator} = ClientSupervisor.coordinator(client)
        state = :sys.get_state(coordinator)

        send(parent, {
          :owned_client,
          client,
          coordinator,
          state.connection_supervisor,
          state.operation_supervisor,
          state.event_fanout.pid
        })

        receive do
          :release -> :ok
        end
      end)

    assert_receive {:owned_client, client, coordinator, connections, operations, fanout}, 500
    pids = [client, coordinator, connections, operations, fanout]
    monitors = Map.new(pids, &{Process.monitor(&1), &1})
    owner_monitor = Process.monitor(owner)

    send(owner, :release)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :normal}, 500

    Enum.each(monitors, fn {monitor, pid} ->
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 500
    end)

    stop_server(server)
  end

  test "refresh reuses a healthy bootstrap connection and close terminates it" do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)

    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])

    first_connection = only_connection(client)
    assert Process.alive?(first_connection)
    assert NativeServer.connection_count(server) == 1

    assert :ok = SDK.refresh_topology(client)
    assert only_connection(client) == first_connection
    assert NativeServer.connection_count(server) == 1

    assert :ok = SDK.close(client)
    refute_eventually(fn -> Process.alive?(first_connection) end)
    assert_eventually(fn -> NativeServer.connection_count(server) == 0 end)
  end

  test "password-only credentials authenticate the default user" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0002, payload: %{"username" => "default", "password" => "secret"}} ->
        "OK"

      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x0001} ->
        %{"auth_required" => true, "protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(seeds: [{"127.0.0.1", port}], password: "secret")

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x0002,
                      payload: %{"username" => "default", "password" => "secret"}
                    }}

    assert :ok = SDK.close(client)
    stop_server(server)
  end

  test "startup refuses an authentication-required session without a password" do
    response_fun = fn
      %{opcode: 0x0001} ->
        NativeServer.startup_payload(%{"auth_required" => true})

      %{opcode: 0x0007, socket: socket} ->
        {:ok, {_address, port}} = :inet.sockname(socket)
        NativeServer.topology_payload(port)

      %{opcode: 0x0002} ->
        "OK"

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)

    assert {:error, :missing_password} =
             SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

    assert_receive {:native_server_request, %{opcode: 0x0001}}
    refute_receive {:native_server_request, %{opcode: 0x0002}}
    refute_receive {:native_server_request, %{opcode: 0x0007}}
    assert_eventually(fn -> NativeServer.connection_count(server) == 0 end)
    stop_server(server)
  end

  @tag capture_log: true
  test "startup waits for the topology management subscription" do
    response_fun = fn
      %{opcode: 0x0011, payload: %{"events" => ["TOPOLOGY_CHANGED"]}} ->
        :noreply

      %{opcode: 0x0007, socket: socket} ->
        {:ok, {_address, port}} = :inet.sockname(socket)
        NativeServer.topology_payload(port)

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)

    startup =
      Task.async(fn ->
        Process.flag(:trap_exit, true)

        SDK.start_link(
          seeds: [{"127.0.0.1", NativeServer.port(server)}],
          topology_refresh_timeout: 40
        )
      end)

    try do
      assert Task.await(startup, 1_000) == {:error, :timeout}
      assert_receive {:native_server_request, %{opcode: 0x0011}}
      assert_eventually(fn -> NativeServer.connection_count(server) == 0 end)
    after
      Task.shutdown(startup, :brutal_kill)
      stop_server(server)
    end
  end

  test "startup flow-control limits bound connection and lane admission" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0001} ->
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

      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x0101} ->
        :noreply

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(seeds: [{"127.0.0.1", port}], connections_per_endpoint: 1)

    connection = only_connection(client)

    first =
      Task.async(fn ->
        SDK.get(client, "first", timeout: :infinity, call_timeout: :infinity)
      end)

    assert_receive {:native_server_request, %{opcode: 0x0101, payload: %{"key" => "first"}}}

    assert {:error, :connection_backpressure} = SDK.get(client, "same-lane")
    assert {:ok, "OK"} = SDK.ping(client, "control-lane")

    state = :sys.get_state(connection)
    assert state.max_in_flight == 2
    assert state.max_in_flight_per_lane == 1
    assert state.pending_lanes == %{1 => 1}

    Task.shutdown(first, :brutal_kill)
    assert :ok = SDK.close(client)
    stop_server(server)
  end

  test "topology refresh has one configurable total deadline" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})

        if request == 1,
          do: NativeServer.topology_payload(Agent.get(port_holder, & &1)),
          else: :noreply

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _ -> NativeServer.port(server) end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        topology_refresh_timeout: 80
      )

    refresh = Task.async(fn -> SDK.refresh_topology(client) end)
    assert Task.yield(refresh, 250) == {:ok, {:error, :timeout}}
    assert ClientRuntime.state(client).topology_manager.refresh_operation == nil
  end

  test "topology refresh replaces a failed connection without exceeding capacity" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, shard_calls} = Agent.start_link(fn -> 0 end)

    response_fun = fn
      %{opcode: 0x0007} ->
        call = Agent.get_and_update(shard_calls, &{&1 + 1, &1 + 1})
        topology = NativeServer.topology_payload(Agent.get(port_holder, & &1))

        case call do
          1 -> topology
          2 -> {:reply, "refresh-failed", status: 1}
          3 -> {:reply_after, 200, topology}
          _later -> topology
        end

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_connections: 1,
        max_connecting: 1,
        connections_per_endpoint: 1
      )

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    refresh = Task.async(fn -> SDK.refresh_topology(client) end)
    assert_eventually(fn -> Agent.get(shard_calls, & &1) >= 3 end)

    assert NativeServer.connection_count(server) <= 1
    assert Task.await(refresh, 1_000) == :ok
    assert NativeServer.connection_count(server) == 1
  end

  test "topology refresh retires a failed endpoint session after an overlapping replacement" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, shard_calls} = Agent.start_link(fn -> 0 end)

    response_fun = fn
      %{opcode: 0x0007} ->
        call = Agent.get_and_update(shard_calls, &{&1 + 1, &1 + 1})
        topology = NativeServer.topology_payload(Agent.get(port_holder, & &1))

        case call do
          1 -> topology
          2 -> {:reply, "refresh-failed", status: 1}
          3 -> {:reply_after, 100, topology}
          _later -> topology
        end

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_connections: 2,
        max_connecting: 1,
        connections_per_endpoint: 1
      )

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    original = only_connection(client)
    refresh = Task.async(fn -> SDK.refresh_topology(client) end)

    assert_eventually(fn -> NativeServer.connection_count(server) == 2 end)
    assert Task.await(refresh, 1_000) == :ok
    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)

    replacement = only_connection(client)
    refute replacement == original
    refute Process.alive?(original)
    assert Process.alive?(replacement)
  end

  test "topology refresh replaces a dead tracked session before its DOWN is handled" do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_connections: 1,
        max_connecting: 1,
        connections_per_endpoint: 1
      )

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    original = only_connection(client)
    :ok = ClientRuntime.suspend(client)
    refresh = Task.async(fn -> SDK.refresh_topology(client) end)

    try do
      assert_eventually(fn ->
        {:message_queue_len, length} =
          Process.info(ClientRuntime.coordinator(client), :message_queue_len)

        length > 0
      end)

      Process.exit(original, :kill)
      refute_eventually(fn -> Process.alive?(original) end)
    after
      :ok = ClientRuntime.resume(client)
    end

    assert Task.await(refresh, 1_000) == :ok
    replacement = only_connection(client)
    refute replacement == original
    assert Process.alive?(replacement)
  end

  @tag capture_log: true
  test "initial topology bootstrap obeys the configured total deadline" do
    {:ok, server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0001} -> :noreply
          _request -> "OK"
        end
      )

    startup =
      Task.async(fn ->
        Process.flag(:trap_exit, true)
        started = System.monotonic_time(:millisecond)

        result =
          SDK.start_link(
            seeds: [{"127.0.0.1", NativeServer.port(server)}],
            topology_refresh_timeout: 40
          )

        {result, System.monotonic_time(:millisecond) - started}
      end)

    try do
      assert {:ok, {{:error, :timeout}, elapsed_ms}} = Task.yield(startup, 250)
      assert elapsed_ms < 200
    after
      Task.shutdown(startup, :brutal_kill)
    end
  end

  @tag capture_log: true
  test "initial topology deadline also bounds endpoint validation" do
    {:ok, server} = NativeServer.start_link(owner: self())

    startup =
      Task.async(fn ->
        Process.flag(:trap_exit, true)
        started = System.monotonic_time(:millisecond)

        result =
          SDK.start_link(
            seeds: [{"127.0.0.1", NativeServer.port(server)}],
            topology_refresh_timeout: 40,
            endpoint_validator: fn _endpoint ->
              Process.sleep(250)
              :ok
            end
          )

        {result, System.monotonic_time(:millisecond) - started}
      end)

    try do
      assert {:ok, {{:error, :timeout}, elapsed_ms}} = Task.yield(startup, 200)
      assert elapsed_ms < 150
    after
      Task.shutdown(startup, :brutal_kill)
    end
  end

  test "topology refresh deadline also bounds endpoint validation" do
    {:ok, validations} = Agent.start_link(fn -> 0 end)
    {:ok, server} = NativeServer.start_link(owner: self())

    validator = fn _endpoint ->
      validation = Agent.get_and_update(validations, &{&1 + 1, &1 + 1})

      if validation > 1, do: Process.sleep(250)
      :ok
    end

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        topology_refresh_timeout: 40,
        endpoint_validator: validator
      )

    refresh =
      Task.async(fn ->
        started = System.monotonic_time(:millisecond)
        result = SDK.refresh_topology(client)
        {result, System.monotonic_time(:millisecond) - started}
      end)

    try do
      assert {:ok, {{:error, :timeout}, elapsed_ms}} = Task.yield(refresh, 200)
      assert elapsed_ms < 150
      assert Process.alive?(client)
    after
      Task.shutdown(refresh, :brutal_kill)
      SDK.close(client)
    end
  end

  @tag capture_log: true
  test "initial topology bootstrap reserves time for later candidates" do
    {:ok, server} = NativeServer.start_link(owner: self())
    healthy_port = NativeServer.port(server)

    validator = fn endpoint ->
      if endpoint.native_port == 1, do: Process.sleep(500)
      :ok
    end

    started = System.monotonic_time(:millisecond)

    assert {:ok, client} =
             SDK.start_link(
               seeds: [{"127.0.0.1", 1}, {"127.0.0.1", healthy_port}],
               endpoint_validator: validator,
               topology_refresh_timeout: 200
             )

    assert System.monotonic_time(:millisecond) - started < 200
    assert :ok = SDK.close(client)
  end

  test "topology refresh reserves time for later candidates" do
    {:ok, slow_validations} = Agent.start_link(fn -> 0 end)
    {:ok, server} = NativeServer.start_link(owner: self())
    healthy_port = NativeServer.port(server)

    validator = fn endpoint ->
      if endpoint.native_port == 1 do
        call = Agent.get_and_update(slow_validations, &{&1 + 1, &1 + 1})
        if call > 1, do: Process.sleep(500)
        {:error, :skip_unreachable_seed}
      else
        :ok
      end
    end

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", 1}, {"127.0.0.1", healthy_port}],
        endpoint_validator: validator,
        topology_refresh_timeout: 200
      )

    started = System.monotonic_time(:millisecond)
    assert :ok = SDK.refresh_topology(client)
    assert System.monotonic_time(:millisecond) - started < 200
    assert :ok = SDK.close(client)
  end

  test "connection startup uses one total validation and handshake deadline" do
    {:ok, server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0001} -> {:reply_after, 35, %{"protocol" => "ferricstore-native"}}
          _request -> "OK"
        end
      )

    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    token = make_ref()

    {:ok, starter} =
      ConnectionStarter.start_link(
        owner: self(),
        token: token,
        key: :deadline_test,
        endpoint: %{
          host: "127.0.0.1",
          native_port: NativeServer.port(server),
          tls: false,
          connect_timeout: 1_000
        },
        connection_supervisor: supervisor,
        client_name: "deadline-test",
        endpoint_validator: fn _endpoint -> Process.sleep(35) end,
        timeout: 50
      )

    monitor = Process.monitor(starter)
    started = System.monotonic_time(:millisecond)

    assert_receive {:ferricstore_connection_started, ^starter, ^token, :deadline_test,
                    {:error, :timeout}},
                   120

    assert System.monotonic_time(:millisecond) - started < 100
    assert_receive {:DOWN, ^monitor, :process, ^starter, :normal}, 50
    DynamicSupervisor.stop(supervisor)
  end

  @tag capture_log: true
  test "startup rejects a server missing the current compute-result contract" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    incompatible_startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "schemas" => %{
            "FETCH_OR_COMPUTE_RESULT" => %{
              "required" => ["key", "value"]
            }
          }
        }
      })

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x0001} -> NativeServer.raw_startup(incompatible_startup)
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _ -> NativeServer.port(server) end)

    startup =
      Task.async(fn ->
        Process.flag(:trap_exit, true)
        SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
      end)

    assert Task.await(startup) ==
             {:error,
              {:incompatible_server_contract,
               %{
                 command: "FETCH_OR_COMPUTE_RESULT",
                 missing_required_fields: ["token", "ttl_ms"]
               }}}
  end

  test "a refresh worker is cancelled when its only caller dies" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})

        if request == 1,
          do: NativeServer.topology_payload(Agent.get(port_holder, & &1)),
          else: :noreply

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _ -> NativeServer.port(server) end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        topology_refresh_timeout: 1_000
      )

    caller = spawn(fn -> SDK.refresh_topology(client) end)

    assert_eventually(fn ->
      not is_nil(ClientRuntime.state(client).topology_manager.refresh_operation)
    end)

    state = ClientRuntime.state(client)
    refresher = state.topology_manager.refresh_operation.refresher

    [{:refresh_call, _from, caller_monitor, _timer, _context}] =
      state.topology_manager.refresh_operation.waiters

    assert state.lifecycle_registry.owners[caller_monitor] ==
             {:refresh_waiter, caller_monitor}

    Process.exit(caller, :kill)

    assert_eventually(fn ->
      ClientRuntime.state(client).topology_manager.refresh_operation == nil
    end)

    refute Process.alive?(refresher)
    assert Process.alive?(client)
  end

  test "a retry refresh is cancelled when its request caller dies" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})

        if request == 1,
          do: NativeServer.topology_payload(Agent.get(port_holder, & &1)),
          else: :noreply

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      %{opcode: 0x0101} ->
        {:reply,
         %{
           "message" => "moved",
           "retryable" => true,
           "safe_to_retry" => true,
           "retry_after_ms" => 0
         }, status: 5}

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
        SDK.get(client, "abandoned-retry", timeout: :infinity, call_timeout: :infinity)
      end)

    assert_eventually(fn ->
      match?(
        %{waiters: [{:request_retry, _tag}]},
        ClientRuntime.state(client).topology_manager.refresh_operation
      )
    end)

    refresher = ClientRuntime.state(client).topology_manager.refresh_operation.refresher
    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      state.request_registry.requests == %{} and
        is_nil(state.topology_manager.refresh_operation)
    end)

    refute Process.alive?(refresher)
    assert Process.alive?(client)
    assert :ok = SDK.close(client)
    stop_server(server)
  end

  test "warm connections queues every topology endpoint behind the global connection bound" do
    data_servers =
      Enum.map(1..3, fn _index ->
        {:ok, server} = NativeServer.start_link(owner: self())
        server
      end)

    ports = Enum.map(data_servers, &NativeServer.port/1)

    {:ok, seed} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} -> three_shard_topology(ports)
          %{opcode: 0x0001} -> %{"protocol" => "ferricstore-native"}
          _request -> "OK"
        end
      )

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed)}],
        warm_connections: true,
        max_connecting: 1
      )

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      Enum.all?(data_servers, &(NativeServer.connection_count(&1) == 1)) and
        map_size(state.connection_pool.connections) == 4 and
        ConnectionPool.connecting_count(state.connection_pool) == 0 and
        :queue.is_empty(state.warmup.queue)
    end)

    state = ClientRuntime.state(client)
    assert map_size(state.connection_pool.connections) == 4
    assert ConnectionPool.connecting_count(state.connection_pool) == 0
    assert :queue.is_empty(state.warmup.queue)

    assert :ok = SDK.close(client)
    Enum.each([seed | data_servers], &stop_server/1)
  end

  test "connection loss resumes a warmup queue when capacity becomes available" do
    data_servers =
      Enum.map(1..3, fn _index ->
        {:ok, server} = NativeServer.start_link(owner: self())
        server
      end)

    ports = Enum.map(data_servers, &NativeServer.port/1)

    {:ok, seed} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} -> three_shard_topology(ports)
          %{opcode: 0x0001} -> %{"protocol" => "ferricstore-native"}
          _request -> "OK"
        end
      )

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed)}],
        warm_connections: true,
        max_connecting: 1,
        max_connections: 2
      )

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      map_size(state.connection_pool.connections) == 2 and
        ConnectionPool.connecting_count(state.connection_pool) == 0 and
        :queue.len(state.warmup.queue) == 2
    end)

    state = ClientRuntime.state(client)
    topology_keys = state.topology_manager.topology.endpoints |> Map.keys() |> MapSet.new()

    {_key, warmed_connection} =
      Enum.find(state.connection_pool.connections, fn {key, _connection} ->
        MapSet.member?(topology_keys, key)
      end)

    Process.exit(warmed_connection, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      map_size(state.connection_pool.connections) == 2 and
        ConnectionPool.connecting_count(state.connection_pool) == 0 and
        :queue.len(state.warmup.queue) == 1
    end)

    assert :ok = SDK.close(client)
    Enum.each([seed | data_servers], &stop_server/1)
  end

  test "a command error does not orphan a newly established routed connection" do
    {:ok, data_server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0101} -> {:reply, "invalid", status: 6}
          _request -> "OK"
        end
      )

    data_port = NativeServer.port(data_server)

    {:ok, seed_server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} -> topology_payload(data_port)
          %{opcode: 0x0001} -> %{"protocol" => "ferricstore-native"}
          _request -> "OK"
        end
      )

    seed_port = NativeServer.port(seed_server)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", seed_port}])

    assert {:error, {:bad_request, "invalid"}} = SDK.get(client, "key")

    assert ClientRuntime.state(client)
           |> Map.fetch!(:connection_pool)
           |> Map.fetch!(:connections)
           |> map_size() == 2

    assert NativeServer.connection_count(seed_server) == 1
    assert NativeServer.connection_count(data_server) == 1

    assert :ok = SDK.close(client)
    assert_eventually(fn -> NativeServer.connection_count(seed_server) == 0 end)
    assert_eventually(fn -> NativeServer.connection_count(data_server) == 0 end)
  end

  test "topology replacement drains an in-flight write before closing its old connection" do
    {:ok, commits} = Agent.start_link(fn -> 0 end)

    {:ok, old_data} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0102} ->
            Agent.update(commits, &(&1 + 1))
            {:reply_after, 200, "OK"}

          %{opcode: 0x0001} ->
            %{"protocol" => "ferricstore-native"}

          _request ->
            "OK"
        end
      )

    {:ok, new_data} = NativeServer.start_link(owner: self())
    old_port = NativeServer.port(old_data)
    new_port = NativeServer.port(new_data)
    {:ok, route_epoch} = Agent.start_link(fn -> 1 end)

    {:ok, seed} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} ->
            epoch = Agent.get(route_epoch, & &1)
            port = if epoch == 1, do: old_port, else: new_port
            NativeServer.topology_payload(port, route_epoch: epoch, node: "data-#{epoch}")

          %{opcode: 0x0001} ->
            %{"protocol" => "ferricstore-native"}

          _request ->
            "OK"
        end
      )

    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(seed)}])

    on_exit(fn ->
      SDK.close(client)

      Enum.each([seed, old_data, new_data], &stop_server/1)
    end)

    write = Task.async(fn -> SDK.set(client, "drain-before-close", "value", timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x0102}}, 500

    old_key = Topology.endpoint_key(%{host: "127.0.0.1", native_port: old_port, tls: false})
    old_connection = ClientRuntime.state(client).connection_pool.connections[old_key]
    assert is_pid(old_connection)

    Agent.update(route_epoch, fn _ -> 2 end)
    assert :ok = SDK.refresh_topology(client)

    refute Map.has_key?(ClientRuntime.state(client).connection_pool.connections, old_key)
    assert Process.alive?(old_connection)
    assert Task.await(write, 1_000) == {:ok, :ok}
    assert Agent.get(commits, & &1) == 1
    refute_eventually(fn -> Process.alive?(old_connection) end)
    assert ClientRuntime.state(client).request_registry.requests == %{}
  end

  test "a retiring topology connection continues to consume capacity until it exits" do
    {:ok, old_data} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0102} -> :noreply
          %{opcode: 0x0001} -> %{"protocol" => "ferricstore-native"}
          _request -> "OK"
        end
      )

    {:ok, new_data} = NativeServer.start_link(owner: self())
    old_port = NativeServer.port(old_data)
    new_port = NativeServer.port(new_data)
    {:ok, route_epoch} = Agent.start_link(fn -> 1 end)

    {:ok, seed} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} ->
            epoch = Agent.get(route_epoch, & &1)
            port = if epoch == 1, do: old_port, else: new_port
            NativeServer.topology_payload(port, route_epoch: epoch, node: "data-#{epoch}")

          %{opcode: 0x0001} ->
            %{"protocol" => "ferricstore-native"}

          _request ->
            "OK"
        end
      )

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed)}],
        warm_connections: true,
        max_connections: 2,
        connections_per_endpoint: 1,
        drain_timeout: 40
      )

    on_exit(fn ->
      SDK.close(client)
      Enum.each([seed, old_data, new_data], &stop_server/1)
    end)

    assert_eventually(fn ->
      NativeServer.connection_count(seed) == 1 and
        NativeServer.connection_count(old_data) == 1
    end)

    write =
      Task.async(fn ->
        SDK.set(client, "held-during-replacement", "value",
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_receive {:native_server_request, %{opcode: 0x0102}}, 500

    old_connection =
      ClientRuntime.state(client)
      |> Map.fetch!(:connection_pool)
      |> Map.fetch!(:connections)
      |> Map.values()
      |> Enum.find(fn connection ->
        :sys.get_state(connection).endpoint.native_port == old_port
      end)

    Agent.update(route_epoch, fn _ -> 2 end)
    assert :ok = SDK.refresh_topology(client)

    assert Process.alive?(old_connection)
    assert NativeServer.connection_count(new_data) == 0
    assert Task.await(write, 500) == {:error, :connection_drained}

    assert_eventually(fn ->
      not Process.alive?(old_connection) and NativeServer.connection_count(new_data) == 1
    end)

    assert NativeServer.connection_count(seed) + NativeServer.connection_count(old_data) +
             NativeServer.connection_count(new_data) <= 2
  end

  test "a read interrupted by a drain deadline is retried on a fresh connection" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, get_calls} = Agent.start_link(fn -> 0 end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      %{opcode: 0x0101} ->
        case Agent.get_and_update(get_calls, &{&1 + 1, &1 + 1}) do
          1 -> :noreply
          _retry -> "retried"
        end

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        drain_timeout: 40,
        max_connections: 2,
        connections_per_endpoint: 2
      )

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    read = Task.async(fn -> SDK.get(client, "read-during-drain", timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x0101}}, 500

    client |> only_connection() |> Connection.drain()

    assert Task.await(read, 1_000) == {:ok, "retried"}
    assert Agent.get(get_calls, & &1) == 2
  end

  test "topology refresh accepts a numerically lower opaque route epoch" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, epochs} = Agent.start_link(fn -> [2, 1] end)

    response_fun = fn
      %{opcode: 0x0007} ->
        epoch =
          Agent.get_and_update(epochs, fn
            [next] -> {next, [next]}
            [next | rest] -> {next, rest}
          end)

        NativeServer.topology_payload(Agent.get(port_holder, & &1), route_epoch: epoch)

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", port}])

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    assert SDK.topology(client).route_epoch == 2

    assert :ok = SDK.refresh_topology(client)
    assert SDK.topology(client).route_epoch == 1
  end

  defp only_connection(client) do
    ClientRuntime.state(client)
    |> Map.fetch!(:connection_pool)
    |> Map.fetch!(:connections)
    |> Map.values()
    |> then(fn [connection] -> connection end)
  end

  defp stop_server(server) do
    if Process.alive?(server), do: GenServer.stop(server, :normal), else: :ok
  catch
    :exit, _reason -> :ok
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())

  defp refute_eventually(fun), do: assert_eventually(fn -> not fun.() end)

  defp topology_payload(port) do
    %{
      "route_epoch" => 1,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1023,
          "shard" => 0,
          "lane_id" => 1,
          "endpoint" => %{
            "node" => "data-node",
            "host" => "127.0.0.1",
            "native_port" => port
          }
        }
      ]
    }
  end

  defp three_shard_topology([first_port, second_port, third_port]) do
    %{
      "route_epoch" => 1,
      "shard_count" => 3,
      "ranges" => [
        topology_range(0, 340, 0, first_port),
        topology_range(341, 681, 1, second_port),
        topology_range(682, 1023, 2, third_port)
      ]
    }
  end

  defp topology_range(first, last, shard, port) do
    %{
      "first_slot" => first,
      "last_slot" => last,
      "shard" => shard,
      "lane_id" => shard + 1,
      "endpoint" => %{
        "node" => "data-#{shard}",
        "host" => "127.0.0.1",
        "native_port" => port
      }
    }
  end
end
