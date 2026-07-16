defmodule FerricStore.SDK.Native.ClientConcurrencyTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Native.ConnectionPool
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  @server_events [
    "AUTH_INVALIDATED",
    "BACKPRESSURE_CHANGED",
    "FLOW_WAKE",
    "GOAWAY",
    "TOPOLOGY_CHANGED"
  ]

  test "async submission is admitted before it can fill the coordinator mailbox" do
    {_server, client} = start_sdk(max_pending_requests: 4)
    :ok = ClientRuntime.suspend(client)

    producers =
      Enum.map(1..2_000, fn index ->
        spawn(fn ->
          FerricStore.Client.async_native(client, :get, %{"key" => "queued-#{index}"})
        end)
      end)

    Process.sleep(50)
    coordinator = ClientRuntime.coordinator(client)
    {:message_queue_len, queued} = Process.info(coordinator, :message_queue_len)

    try do
      assert queued <= 4
    after
      Enum.each(producers, &Process.exit(&1, :kill))
      :ok = ClientRuntime.resume(client)
    end
  end

  test "synchronous submission is admitted before it can fill the coordinator mailbox" do
    {_server, client} = start_sdk(max_pending_requests: 4)
    :ok = ClientRuntime.suspend(client)

    callers =
      Enum.map(1..2_000, fn index ->
        spawn(fn ->
          SDK.get(client, "sync-queued-#{index}",
            timeout: :infinity,
            call_timeout: :infinity
          )
        end)
      end)

    Process.sleep(50)
    coordinator = ClientRuntime.coordinator(client)
    {:message_queue_len, queued} = Process.info(coordinator, :message_queue_len)

    try do
      assert queued <= 4
    after
      Enum.each(callers, &Process.exit(&1, :kill))
      :ok = ClientRuntime.resume(client)
    end
  end

  test "a timed-out async admission produces exactly one coordinator result" do
    {_server, client} = start_sdk()
    :ok = ClientRuntime.suspend(client)

    request =
      FerricStore.Client.async_native(
        client,
        :get,
        %{"key" => "delayed-admission"},
        timeout: 1_000,
        call_timeout: 20
      )

    :ok = ClientRuntime.resume(client)

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             FerricStore.Client.await(request, 500)

    ref = request.ref
    refute_receive {FerricStore.AsyncRequest, ^ref, _result}, 100
  end

  test "normal routed traffic uses bounded client pending state without per-call tasks" do
    {server, client} = start_sdk(response_delay: 150, max_pending_requests: 64)
    started = System.monotonic_time(:millisecond)

    tasks =
      Enum.map(1..20, fn index ->
        Task.async(fn -> SDK.get(client, "key-#{index}", timeout: 1_000) end)
      end)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)
      map_size(state.request_registry.requests) == 20
    end)

    assert Task.await_many(tasks, 2_000) == List.duplicate({:ok, "OK"}, 20)
    assert System.monotonic_time(:millisecond) - started < 400
    assert ClientRuntime.state(client).request_registry.requests == %{}
    assert NativeServer.connection_count(server) == 2
  end

  test "a busy endpoint opens another session instead of head-of-line blocking" do
    {server, client} = start_sdk(connections_per_endpoint: 2)
    first_connection = only_connection(client)
    :ok = :sys.suspend(first_connection)

    blocked =
      Task.async(fn ->
        SDK.set(client, "blocked-write", "value", timeout: 1_000, call_timeout: :infinity)
      end)

    try do
      assert_eventually(fn ->
        Enum.any?(ClientRuntime.state(client).request_registry.requests, fn {_tag, request} ->
          request.conn == first_connection
        end)
      end)

      assert {:ok, "OK"} =
               SDK.ping(client, "second-session", timeout: 500, call_timeout: 1_000)

      assert_eventually(fn -> NativeServer.connection_count(server) == 2 end)
    after
      :ok = :sys.resume(first_connection)
    end

    assert {:ok, :ok} = Task.await(blocked, 1_000)
  end

  test "a response tag from the wrong connection cannot consume a pending request" do
    {_server, client} = start_sdk(response_delay: 80)

    request = Task.async(fn -> SDK.get(client, "protected-tag", timeout: 500) end)

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    [{tag, _pending}] = ClientRuntime.state(client).request_registry.requests |> Map.to_list()

    send(
      ClientRuntime.coordinator(client),
      {:ferricstore_connection_response, self(), tag, {:ok, "forged"}}
    )

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    assert Task.await(request, 1_000) == {:ok, "OK"}
    assert ClientRuntime.state(client).request_registry.requests == %{}
  end

  test "a client-side call timeout cancels the underlying connection request" do
    {_server, client} = start_sdk(response_delay: 5_000)
    connection = only_connection(client)

    assert {:error, :timeout} =
             SDK.get(client, "cancel-me", timeout: 1_000, call_timeout: 60)

    assert_eventually(fn -> ClientRuntime.state(client).request_registry.requests == %{} end)

    assert_eventually(fn ->
      state = :sys.get_state(connection)

      state.pending_targets == %{} and map_size(state.pending) == 1 and
        Enum.all?(state.pending, fn {_request_id, pending} -> pending.phase == :discarding end)
    end)

    assert Process.alive?(connection)
  end

  test "request timeout cleanup cannot block the coordinator on a suspended connection" do
    {_server, client} = start_sdk()
    connection = only_connection(client)
    :ok = :sys.suspend(connection)

    on_exit(fn ->
      if Process.alive?(connection) and
           Process.info(connection, :status) == {:status, :suspended} do
        :ok = :sys.resume(connection)
      end
    end)

    request =
      Task.async(fn ->
        SDK.get(client, "nonblocking-timeout-cleanup", timeout: 5_000, call_timeout: :infinity)
      end)

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    [{tag, _request}] =
      client
      |> ClientRuntime.state()
      |> Map.fetch!(:request_registry)
      |> Map.fetch!(:requests)
      |> Map.to_list()

    send(ClientRuntime.coordinator(client), {:pending_request_timeout, tag})
    topology = Task.async(fn -> SDK.topology(client) end)

    topology_result =
      try do
        Task.yield(topology, 100)
      after
        :ok = :sys.resume(connection)
      end

    if is_nil(topology_result), do: Task.shutdown(topology, :brutal_kill)

    assert {:ok, %FerricStore.SDK.Native.Topology{}} = topology_result
    assert Task.await(request, 500) == {:error, :timeout}
  end

  test "a call that expires in the coordinator mailbox never executes later" do
    {_server, client} = start_sdk()
    flush_native_server_messages()
    :ok = ClientRuntime.suspend(client)

    caller =
      Task.async(fn ->
        SDK.set(client, "expired-in-mailbox", "must-not-arrive",
          timeout: 1_000,
          call_timeout: 60
        )
      end)

    assert Task.await(caller, 250) == {:error, :timeout}
    :ok = ClientRuntime.resume(client)

    refute_receive {:native_server_request,
                    %{opcode: 0x0102, payload: %{"key" => "expired-in-mailbox"}}},
                   150

    assert Process.alive?(client)
    assert ClientRuntime.state(client).request_registry.requests == %{}
  end

  test "a call that expires in the connection mailbox never executes later" do
    {_server, client} = start_sdk()
    connection = only_connection(client)
    flush_native_server_messages()
    :ok = :sys.suspend(connection)

    result =
      try do
        SDK.set(client, "expired-in-connection-mailbox", "must-not-arrive", timeout: 40)
      after
        :ok = :sys.resume(connection)
      end

    assert result == {:error, :timeout}

    refute_receive {:native_server_request,
                    %{
                      opcode: 0x0102,
                      payload: %{"key" => "expired-in-connection-mailbox"}
                    }},
                   150

    assert Process.alive?(client)
    assert ClientRuntime.state(client).request_registry.requests == %{}
  end

  @tag capture_log: true
  test "a read retries once when its socket closes after the request was sent" do
    {:ok, get_attempts} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0101}, _port ->
        attempt = Agent.get_and_update(get_attempts, fn count -> {count + 1, count + 1} end)
        if attempt == 1, do: :close, else: "recovered"

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)

    assert {:ok, "recovered"} = SDK.get(client, "retry-after-close", timeout: 500)
    assert Agent.get(get_attempts, & &1) == 2
  end

  @tag capture_log: true
  test "a stale timeout from the first attempt cannot cancel a retried request" do
    test_pid = self()
    {:ok, get_attempts} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0101}, _port ->
        attempt = Agent.get_and_update(get_attempts, fn count -> {count + 1, count + 1} end)

        if attempt == 1 do
          send(test_pid, {:first_attempt_dispatched, self()})

          receive do
            :close_first_attempt -> :close
          end
        else
          {:reply_after, 100, "recovered"}
        end

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    request = Task.async(fn -> SDK.get(client, "retry-timeout-race", timeout: 1_000) end)

    assert_receive {:first_attempt_dispatched, server_handler}, 250

    [{first_tag, _first_request}] =
      Map.to_list(ClientRuntime.state(client).request_registry.requests)

    send(server_handler, :close_first_attempt)

    assert_eventually(fn ->
      case Map.to_list(ClientRuntime.state(client).request_registry.requests) do
        [{retry_tag, _retry_request}] -> retry_tag != first_tag
        _other -> false
      end
    end)

    send(ClientRuntime.coordinator(client), {:pending_request_timeout, first_tag})
    assert Task.await(request, 1_500) == {:ok, "recovered"}
  end

  @tag capture_log: true
  test "an async retry uses a fresh internal tag so a stale timeout cannot cancel it" do
    test_pid = self()
    {:ok, get_attempts} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0101}, _port ->
        attempt = Agent.get_and_update(get_attempts, fn count -> {count + 1, count + 1} end)

        if attempt == 1 do
          send(test_pid, {:async_first_attempt_dispatched, self()})

          receive do
            :close_first_attempt -> :close
          end
        else
          {:reply_after, 100, "recovered"}
        end

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)

    request =
      FerricStore.Client.async_native(
        client,
        :get,
        %{"key" => "async-retry-timeout-race"},
        key: "async-retry-timeout-race",
        timeout: 1_000
      )

    assert_receive {:async_first_attempt_dispatched, server_handler}, 250

    [{first_tag, _first_request}] =
      Map.to_list(ClientRuntime.state(client).request_registry.requests)

    send(server_handler, :close_first_attempt)

    assert_eventually(fn ->
      case Map.to_list(ClientRuntime.state(client).request_registry.requests) do
        [{retry_tag, _retry_request}] -> retry_tag != first_tag
        _other -> false
      end
    end)

    send(ClientRuntime.coordinator(client), {:pending_request_timeout, first_tag})
    assert FerricStore.Client.await(request, 1_500) == "recovered"
  end

  test "GOAWAY drains a correlated write response before retiring the connection" do
    {:ok, commits} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0102}, _port ->
        Agent.update(commits, &(&1 + 1))
        {:reply_after, 100, "OK"}

      _request, _port ->
        :default
    end

    {server, client} = start_sdk(response_handler: response_handler)
    connection = only_connection(client)

    write = Task.async(fn -> SDK.set(client, "drain-correlated-write", "value") end)
    assert_receive {:native_server_request, %{opcode: 0x0102}}, 500

    assert [:ok] =
             NativeServer.send_event(server, %{"reason" => "maintenance"}, opcode: 0x000A)

    assert Task.await(write, 1_000) == {:ok, :ok}
    assert Agent.get(commits, & &1) == 1
    assert_eventually(fn -> not Process.alive?(connection) end)
    assert Process.alive?(client)
  end

  test "an infinite request is cancelled when its caller terminates" do
    {_server, client} = start_sdk(response_delay: 5_000)
    connection = only_connection(client)

    caller =
      spawn(fn ->
        SDK.get(client, "abandoned-request", timeout: :infinity, call_timeout: :infinity)
      end)

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)
      connection_state = :sys.get_state(connection)

      state.request_registry.requests == %{} and pending_request_monitor_index_empty?(state) and
        connection_state.pending_targets == %{} and map_size(connection_state.pending) == 1 and
        Enum.all?(connection_state.pending, fn {_request_id, pending} ->
          pending.phase == :discarding
        end)
    end)
  end

  test "a finite request is cancelled promptly when its caller terminates" do
    {_server, client} = start_sdk(response_delay: 5_000)
    connection = only_connection(client)

    caller =
      spawn(fn ->
        SDK.get(client, "abandoned-finite-request", timeout: 1_000, call_timeout: :infinity)
      end)

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)

    state = ClientRuntime.state(client)
    [{tag, pending_request}] = Map.to_list(state.request_registry.requests)

    assert state.lifecycle_registry.owners[pending_request.caller_monitor] ==
             {:pending_request, tag}

    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)
      connection_state = :sys.get_state(connection)

      state.request_registry.requests == %{} and pending_request_monitor_index_empty?(state) and
        connection_state.pending_targets == %{} and map_size(connection_state.pending) == 1 and
        Enum.all?(connection_state.pending, fn {_request_id, pending} ->
          pending.phase == :discarding
        end)
    end)
  end

  test "a slow routed connection handshake does not block traffic on an existing connection" do
    {seed, data, client, seed_endpoint} = start_sdk_with_slow_routed_endpoint(300)
    flush_native_server_messages()

    routed_request = Task.async(fn -> SDK.get(client, "slow-route", timeout: 1_000) end)
    assert_receive {:native_server_request, %{opcode: 0x000C}}, 200

    started = System.monotonic_time(:millisecond)

    assert {:ok, "OK"} =
             SDK.ping(client, "still-responsive", endpoint: seed_endpoint, timeout: 500)

    assert System.monotonic_time(:millisecond) - started < 150
    assert Task.await(routed_request, 1_500) == {:ok, "OK"}

    stop_sdk(client, [seed, data])
  end

  test "request timeout still bounds connection preparation with an infinite call timeout" do
    {:ok, slow} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x000C} ->
            {:reply_after, 200, %{"protocol" => "ferricstore-native"}}

          %{opcode: 0x0101} ->
            "late-success"

          _request ->
            "OK"
        end
      )

    {:ok, seed} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(seed)}])

    on_exit(fn -> stop_sdk(client, [seed, slow]) end)
    flush_native_server_messages()
    started = System.monotonic_time(:millisecond)

    assert {:error, :timeout} =
             SDK.request(
               client,
               :get,
               %{"key" => "must-expire-during-connect"},
               endpoint: %{host: "127.0.0.1", native_port: NativeServer.port(slow)},
               timeout: 40,
               call_timeout: :infinity
             )

    assert System.monotonic_time(:millisecond) - started < 150
    refute_receive {:native_server_request, %{opcode: 0x0101}}, 250
  end

  test "a connection starter is cancelled when its last waiter times out" do
    {:ok, slow} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x000C} -> :noreply
          _request -> "OK"
        end
      )

    {:ok, seed} = NativeServer.start_link(owner: self())
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(seed)}])
    on_exit(fn -> stop_sdk(client, [seed, slow]) end)

    assert {:error, :timeout} =
             SDK.request(
               client,
               :get,
               %{"key" => "cancel-empty-starter"},
               endpoint: %{host: "127.0.0.1", native_port: NativeServer.port(slow)},
               timeout: 40
             )

    assert_eventually(fn ->
      ClientRuntime.state(client)
      |> Map.fetch!(:connection_pool)
      |> ConnectionPool.connecting_count() ==
        0
    end)

    assert_eventually(fn -> NativeServer.connection_count(slow) == 0 end)
  end

  test "concurrent connection establishment has a global client bound" do
    slow_servers =
      Enum.map(1..3, fn _index ->
        {:ok, server} =
          NativeServer.start_link(
            owner: self(),
            response_fun: fn
              %{opcode: 0x000C} -> :noreply
              _request -> "OK"
            end
          )

        server
      end)

    {:ok, seed} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed)}],
        max_connecting: 2
      )

    on_exit(fn -> stop_sdk(client, [seed | slow_servers]) end)

    requests =
      Enum.map(slow_servers, fn server ->
        Task.async(fn ->
          SDK.request(
            client,
            :get,
            %{"key" => "bounded-connect"},
            endpoint: %{host: "127.0.0.1", native_port: NativeServer.port(server)},
            timeout: 500
          )
        end)
      end)

    assert_eventually(fn ->
      ClientRuntime.state(client)
      |> Map.fetch!(:connection_pool)
      |> ConnectionPool.connecting_count() >=
        2
    end)

    Process.sleep(30)

    assert ClientRuntime.state(client)
           |> Map.fetch!(:connection_pool)
           |> ConnectionPool.connecting_count() <= 2

    Enum.each(requests, &Task.shutdown(&1, :brutal_kill))
  end

  test "a slow endpoint validator does not block unrelated client traffic" do
    parent = self()
    {:ok, target} = NativeServer.start_link(owner: self())
    target_port = NativeServer.port(target)
    {:ok, seed} = NativeServer.start_link(owner: self())
    seed_endpoint = %{host: "127.0.0.1", native_port: NativeServer.port(seed)}

    validator = fn endpoint ->
      if endpoint.native_port == target_port do
        send(parent, :slow_validator_entered)
        Process.sleep(250)
      end

      :ok
    end

    {:ok, client} =
      SDK.start_link(seeds: [seed_endpoint], endpoint_validator: validator)

    on_exit(fn -> stop_sdk(client, [seed, target]) end)

    request =
      Task.async(fn ->
        SDK.request(
          client,
          :get,
          %{"key" => "validated-endpoint"},
          endpoint: %{host: "127.0.0.1", native_port: target_port},
          timeout: 1_000
        )
      end)

    assert_receive :slow_validator_entered, 200
    state = ClientRuntime.state(client)

    assert Enum.any?(
             DynamicSupervisor.which_children(state.operation_supervisor),
             fn {_id, _pid, :worker, modules} ->
               modules == [FerricStore.SDK.Native.ConnectionStarter]
             end
           )

    assert Enum.all?(
             DynamicSupervisor.which_children(state.connection_supervisor),
             fn {_id, _pid, :worker, modules} ->
               modules == [FerricStore.SDK.Native.Connection]
             end
           )

    started = System.monotonic_time(:millisecond)

    assert {:ok, "OK"} =
             SDK.ping(client, "validator-isolated", endpoint: seed_endpoint, timeout: 500)

    assert System.monotonic_time(:millisecond) - started < 150
    assert Task.await(request, 1_000) == {:ok, "OK"}
  end

  test "an endpoint validator exception becomes a request error without killing the client" do
    {:ok, target} = NativeServer.start_link(owner: self())
    target_port = NativeServer.port(target)
    {:ok, seed} = NativeServer.start_link(owner: self())

    validator = fn endpoint ->
      if endpoint.native_port == target_port, do: raise("validator exploded")
      :ok
    end

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(seed)}],
        endpoint_validator: validator
      )

    on_exit(fn -> stop_sdk(client, [seed, target]) end)

    assert {:error, {:endpoint_validator_failed, {:error, "validator exploded"}}} =
             SDK.request(
               client,
               :get,
               %{"key" => "invalid-endpoint"},
               endpoint: %{host: "127.0.0.1", native_port: target_port},
               timeout: 500
             )

    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.ping(client)
  end

  test "a write whose client deadline expires while connecting is never sent" do
    {seed, data, client, _seed_endpoint} = start_sdk_with_slow_routed_endpoint(250)
    flush_native_server_messages()

    caller =
      Task.async(fn ->
        try do
          SDK.set(client, "must-not-arrive", "value", timeout: 1_000, call_timeout: 60)
        catch
          :exit, {:timeout, _call} -> :caller_timed_out
        end
      end)

    result = Task.await(caller, 500)

    write_sent? =
      receive do
        {:native_server_request, %{opcode: 0x0102, payload: %{"key" => "must-not-arrive"}}} ->
          true
      after
        400 -> false
      end

    assert {result, write_sent?} == {{:error, :timeout}, false}

    stop_sdk(client, [seed, data])
  end

  test "topology refresh runs without blocking routed traffic" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0007}, port ->
        request_number = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})
        topology = NativeServer.topology_payload(port)
        if request_number == 1, do: topology, else: {:reply_after, 300, topology}

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    flush_native_server_messages()
    refresh = Task.async(fn -> SDK.refresh_topology(client) end)
    assert_receive {:native_server_request, %{opcode: 0x0007}}, 200
    started = System.monotonic_time(:millisecond)

    assert {:ok, "OK"} = SDK.get(client, "during-refresh")
    assert System.monotonic_time(:millisecond) - started < 150
    assert Task.await(refresh, 1_000) == :ok
  end

  test "topology refresh callers share the global pending-work limit" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0007}, port ->
        case Agent.get_and_update(shard_requests, &{&1, &1 + 1}) do
          0 -> NativeServer.topology_payload(port)
          _later -> :noreply
        end

      _request, _port ->
        :default
    end

    {_server, client} =
      start_sdk(response_handler: response_handler, max_pending_requests: 2)

    admitted =
      Enum.map(1..2, fn _index ->
        Task.async(fn -> SDK.refresh_topology(client) end)
      end)

    assert_eventually(fn -> refresh_waiter_count(client) == 2 end)
    rejected = Task.async(fn -> SDK.refresh_topology(client) end)
    result = Task.yield(rejected, 200)

    Task.shutdown(rejected, :brutal_kill)
    Enum.each(admitted, &Task.shutdown(&1, :brutal_kill))

    assert result == {:ok, {:error, :client_backpressure}}

    assert_eventually(fn ->
      is_nil(ClientRuntime.state(client).topology_manager.refresh_operation)
    end)
  end

  test "full client inspection redacts credentials in pending payloads" do
    response_handler = fn
      %{opcode: 0x0002}, _port -> {:reply_after, 200, "OK"}
      _request, _port -> :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)

    auth =
      Task.async(fn ->
        SDK.request(client, :auth, %{
          "username" => "inspect-user",
          "password" => "never-print-this-secret"
        })
      end)

    assert_eventually(fn ->
      map_size(ClientRuntime.state(client).request_registry.requests) == 1
    end)

    rendered = inspect(ClientRuntime.state(client), limit: :infinity, printable_limit: :infinity)

    refute rendered =~ "never-print-this-secret"
    assert rendered =~ "[REDACTED]"
    assert Task.await(auth, 500) == {:ok, "OK"}
  end

  test "event subscription delivers request-id zero frames and GOAWAY reconnects cleanly" do
    {server, client} = start_sdk()
    first_connection = only_connection(client)

    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}}

    payload = %{"event" => "FLOW_WAKE", "payload" => %{"credit" => 1}, "at_ms" => 1}
    assert [:ok] = NativeServer.send_event(server, payload)

    assert {:ok, %{opcode: 0x0010, name: "EVENT", value: ^payload}} =
             SDK.await_event(client, 500)

    assert [:ok] =
             NativeServer.send_event(server, %{"reason" => "maintenance"}, opcode: 0x000A)

    assert {:ok, %{opcode: 0x000A, name: "GOAWAY", value: %{"reason" => "maintenance"}}} =
             SDK.await_event(client, 500)

    assert_eventually(fn -> not Process.alive?(first_connection) end)
    assert {:ok, "OK"} = SDK.get(client, "after-goaway")
    refute only_connection(client) == first_connection

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x0011,
                      payload: %{"events" => ["FLOW_WAKE", "TOPOLOGY_CHANGED"]}
                    }}

    assert {:ok, "OK"} = SDK.unsubscribe_events(client, ["flow_wake"])
  end

  test "the topology client maintains a management subscription and refreshes on change" do
    {:ok, route_epoch} = Agent.start_link(fn -> 1 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1),
          route_epoch: Agent.get(route_epoch, & &1)
        )

      %{opcode: 0x000C} ->
        NativeServer.startup_payload()

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _ -> NativeServer.port(server) end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["TOPOLOGY_CHANGED"]}}},
                   500

    Agent.update(route_epoch, fn _ -> 2 end)
    event = %{"event" => "TOPOLOGY_CHANGED", "payload" => %{"route_epoch" => 2}}

    assert_eventually(fn -> NativeServer.send_event(server, event) == [:ok] end)
    assert_eventually(fn -> SDK.topology(client).route_epoch == 2 end)
  end

  test "a topology event during refresh schedules a snapshot after the in-flight one" do
    {:ok, shard_requests} = Agent.start_link(fn -> 0 end)
    {:ok, route_epoch} = Agent.start_link(fn -> 1 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(shard_requests, &{&1 + 1, &1 + 1})
        port = Agent.get(port_holder, & &1)

        case request do
          1 ->
            {:reply_after, 100, NativeServer.topology_payload(port, route_epoch: 1)}

          _other ->
            NativeServer.topology_payload(port, route_epoch: Agent.get(route_epoch, & &1))
        end

      %{opcode: 0x000C} ->
        NativeServer.startup_payload()

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _ -> NativeServer.port(server) end)
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["TOPOLOGY_CHANGED"]}}},
                   500

    flush_native_server_messages()
    refresh = Task.async(fn -> SDK.refresh_topology(client) end)
    assert_receive {:native_server_request, %{opcode: 0x0007}}, 200

    Agent.update(route_epoch, fn _ -> 2 end)
    event = %{"event" => "TOPOLOGY_CHANGED", "payload" => %{"route_epoch" => 2}}
    assert NativeServer.send_event(server, event) == [:ok]

    assert Task.await(refresh, 500) == :ok
    assert_eventually(fn -> Agent.get(shard_requests, & &1) >= 3 end)
    assert_eventually(fn -> SDK.topology(client).route_epoch == 2 end)
  end

  test "event waits select the originating client" do
    {first_server, first_client} = start_sdk()
    {second_server, second_client} = start_sdk()

    assert {:ok, "OK"} = SDK.subscribe_events(first_client, ["flow_wake"])
    assert {:ok, "OK"} = SDK.subscribe_events(second_client, ["flow_wake"])

    second_event = %{"event" => "FLOW_WAKE", "payload" => %{"client" => 2}, "at_ms" => 2}
    first_event = %{"event" => "FLOW_WAKE", "payload" => %{"client" => 1}, "at_ms" => 1}

    assert [:ok] = NativeServer.send_event(second_server, second_event)
    assert [:ok] = NativeServer.send_event(first_server, first_event)

    assert {:ok, %{value: ^first_event}} = SDK.await_event(first_client, 500)
    assert {:ok, %{value: ^second_event}} = SDK.await_event(second_client, 500)
    refute_receive {:ferricstore_event, _event}
  end

  test "event subscriptions are reference-counted across local subscribers" do
    {_server, client} = start_sdk()
    flush_native_server_messages()
    second_subscriber = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(second_subscriber), do: Process.exit(second_subscriber, :kill)
    end)

    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}}

    assert {:ok, "OK"} =
             SDK.subscribe_events(client, ["flow_wake"], subscriber: second_subscriber)

    refute_receive {:native_server_request, %{opcode: 0x0011}}, 50

    assert {:ok, "OK"} = SDK.unsubscribe_events(client, ["flow_wake"])
    refute_receive {:native_server_request, %{opcode: 0x0012}}, 50

    assert {:ok, "OK"} =
             SDK.unsubscribe_events(client, ["flow_wake"], subscriber: second_subscriber)

    assert_receive {:native_server_request,
                    %{opcode: 0x0012, payload: %{"events" => ["FLOW_WAKE"]}}}
  end

  test "event subscriber state is bounded independently of request throughput" do
    {_server, client} = start_sdk(max_event_subscribers: 1)
    second_subscriber = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(second_subscriber), do: Process.exit(second_subscriber, :kill)
    end)

    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0011}}

    assert {:error, :event_subscriber_backpressure} =
             SDK.subscribe_events(client, ["topology_changed"], subscriber: second_subscriber)

    refute_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["TOPOLOGY_CHANGED"]}}},
                   100
  end

  test "subscriber termination releases its server-side event interests" do
    {_server, client} = start_sdk()
    subscriber = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, "OK"} =
             SDK.subscribe_events(client, ["flow_wake"], subscriber: subscriber)

    assert_receive {:native_server_request, %{opcode: 0x0011}}
    Process.exit(subscriber, :kill)

    assert_receive {:native_server_request,
                    %{opcode: 0x0012, payload: %{"events" => ["FLOW_WAKE"]}}}

    assert_eventually(fn ->
      ClientRuntime.state(client).event_coordinator.subscriptions.subscribers == %{}
    end)

    assert ClientRuntime.state(client).event_coordinator.subscriptions.refcounts == %{}
  end

  test "a failed dead-subscriber cleanup retires the stale event session" do
    response_handler = fn
      %{opcode: 0x0012}, _port -> {:reply, "unsubscribe-failed", status: 1}
      _request, _port -> :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    subscriber = spawn(fn -> Process.sleep(:infinity) end)

    assert {:ok, "OK"} =
             SDK.subscribe_events(client, ["flow_wake"], subscriber: subscriber)

    assert_receive {:native_server_request, %{opcode: 0x0011}}
    stale_connection = ClientRuntime.state(client).event_coordinator.subscriptions.connection

    Process.exit(subscriber, :kill)

    assert_receive {:native_server_request,
                    %{opcode: 0x0012, payload: %{"events" => ["FLOW_WAKE"]}}}

    assert_eventually(fn -> not Process.alive?(stale_connection) end)

    assert_eventually(fn ->
      subscriptions = ClientRuntime.state(client).event_coordinator.subscriptions

      subscriptions.subscribers == %{} and subscriptions.refcounts == %{} and
        is_pid(subscriptions.connection) and Process.alive?(subscriptions.connection)
    end)
  end

  test "queued event subscriptions expire before dispatch and do not change local state" do
    response_handler = fn
      %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}, _port ->
        {:reply_after, 200, "OK"}

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    subscriber = self()

    first =
      Task.async(fn ->
        SDK.subscribe_events(client, ["flow_wake"], subscriber: subscriber, call_timeout: 500)
      end)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}}

    second =
      Task.async(fn ->
        try do
          SDK.subscribe_events(client, ["goaway"], subscriber: subscriber, call_timeout: 60)
        catch
          :exit, {:timeout, _call} -> :caller_timed_out
        end
      end)

    second_result = Task.await(second, 200)
    assert Task.await(first, 500) == {:ok, "OK"}

    refute_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["GOAWAY"]}}},
                   100

    assert second_result == {:error, :timeout}

    assert ClientRuntime.state(client).event_coordinator.subscriptions.refcounts == %{
             "FLOW_WAKE" => 1
           }
  end

  test "a queued event subscription is removed when its caller terminates" do
    response_handler = fn
      %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}, _port ->
        {:reply_after, 200, "OK"}

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    subscriber = self()

    first =
      Task.async(fn ->
        SDK.subscribe_events(client, ["flow_wake"], subscriber: subscriber, call_timeout: 500)
      end)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}}

    abandoned =
      spawn(fn ->
        SDK.subscribe_events(client, ["goaway"],
          subscriber: subscriber,
          call_timeout: :infinity
        )
      end)

    assert_eventually(fn -> event_queue_size(client) == 1 end)

    state = ClientRuntime.state(client)
    [queued_call] = event_queue_calls(state.event_coordinator.queue)

    assert state.lifecycle_registry.owners[queued_call.caller_monitor] ==
             {:event_call, queued_call.id}

    Process.exit(abandoned, :kill)
    assert_eventually(fn -> event_queue_size(client) == 0 end)
    assert Task.await(first, 500) == {:ok, "OK"}

    refute_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["GOAWAY"]}}},
                   100

    assert ClientRuntime.state(client).event_coordinator.subscriptions.refcounts == %{
             "FLOW_WAKE" => 1
           }
  end

  test "queued event caller cancellation scales near-linearly" do
    event_cancel_reductions(12)
    small = event_cancel_reductions(40)
    large = event_cancel_reductions(80)

    assert large < small * 3,
           "expected near-linear cancellation reductions, got #{small} for 40 and #{large} for 80"
  end

  test "refresh caller cancellation scales near-linearly" do
    refresh_cancel_reductions(12)
    small = refresh_cancel_reductions(40)
    large = refresh_cancel_reductions(80)

    assert large < small * 3,
           "expected near-linear cancellation reductions, got #{small} for 40 and #{large} for 80"
  end

  test "events are delivered only to subscribers interested in their kind" do
    {server, client} = start_sdk()
    parent = self()
    sleeper = spawn(fn -> forward_events(parent) end)

    on_exit(fn ->
      if Process.alive?(sleeper), do: Process.exit(sleeper, :kill)
    end)

    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0011}}

    assert {:ok, "OK"} =
             SDK.subscribe_events(client, ["backpressure_changed"], subscriber: sleeper)

    assert_receive {:native_server_request, %{opcode: 0x0011}}

    wake = %{"event" => "FLOW_WAKE", "payload" => %{}, "at_ms" => 1}
    assert [:ok] = NativeServer.send_event(server, wake)
    assert {:ok, %{value: ^wake}} = SDK.await_event(client, 500)
    refute_receive {:forwarded_event, ^sleeper, %{value: ^wake}}, 50

    pressure = %{"event" => "BACKPRESSURE_CHANGED", "payload" => %{}, "at_ms" => 2}
    assert [:ok] = NativeServer.send_event(server, pressure)
    assert_receive {:forwarded_event, ^sleeper, %{value: ^pressure}}, 500
    refute_receive {:ferricstore_event, ^client, %{value: ^pressure}}, 50
  end

  test "a failed event restore is retried until desired subscriptions are active" do
    {:ok, subscribe_attempts} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0011, payload: %{"events" => events}}, _port ->
        attempt = Agent.get_and_update(subscribe_attempts, &{&1 + 1, &1 + 1})

        if attempt == 3 and events == ["FLOW_WAKE", "TOPOLOGY_CHANGED"],
          do: {:reply, "restore-failed", status: 1},
          else: "OK"

      _request, _port ->
        :default
    end

    {server, client} = start_sdk(response_handler: response_handler)
    first_connection = only_connection(client)

    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0011}}
    assert [:ok] = NativeServer.send_event(server, %{"reason" => "restart"}, opcode: 0x000A)
    assert {:ok, %{opcode: 0x000A}} = SDK.await_event(client, 500)
    assert_eventually(fn -> not Process.alive?(first_connection) end)

    assert {:ok, "OK"} = SDK.get(client, "trigger-reconnect")

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      Agent.get(subscribe_attempts, & &1) >= 4 and
        is_pid(state.event_coordinator.subscriptions.connection)
    end)

    wake = %{"event" => "FLOW_WAKE", "payload" => %{}, "at_ms" => 1}
    assert [:ok] = NativeServer.send_event(server, wake)
    assert {:ok, %{value: ^wake}} = SDK.await_event(client, 500)
  end

  @tag capture_log: true
  test "an ordinary event socket disconnect reconnects and restores subscriptions" do
    {:ok, subscribe_attempts} = Agent.start_link(fn -> 0 end)
    owner = self()

    response_handler = fn
      %{opcode: 0x0011, socket: socket}, _port ->
        attempt = Agent.get_and_update(subscribe_attempts, &{&1 + 1, &1 + 1})

        if attempt == 1, do: send(owner, {:initial_event_socket, socket})

        "OK"

      _request, _port ->
        :default
    end

    {server, client} = start_sdk(response_handler: response_handler)
    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0011}}, 200
    assert_receive {:initial_event_socket, event_socket}, 200
    initial_connection = ClientRuntime.state(client).event_coordinator.subscriptions.connection
    :ok = :gen_tcp.close(event_socket)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}},
                   700

    assert_eventually(fn ->
      state = ClientRuntime.state(client)
      restored_connection = state.event_coordinator.subscriptions.connection

      Agent.get(subscribe_attempts, & &1) >= 2 and
        is_pid(restored_connection) and restored_connection != initial_connection and
        Process.alive?(restored_connection)
    end)

    event = %{"event" => "FLOW_WAKE", "payload" => %{}, "at_ms" => 1}
    assert [_result] = NativeServer.send_event(server, event)
    assert {:ok, %{value: ^event}} = SDK.await_event(client, 500)
  end

  test "an idle event session sends heartbeats before the server idle timeout" do
    {_server, client} = start_sdk(heartbeat_interval: 25)
    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    flush_native_server_messages()

    assert_receive {:native_server_request, %{opcode: 0x0003}}, 150
    assert Process.alive?(ClientRuntime.state(client).event_coordinator.subscriptions.connection)
  end

  test "heartbeats start only after the session bootstrap succeeds" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn request ->
      port = Agent.get(port_holder, & &1)

      case request.opcode do
        0x000C -> {:reply_after, 80, %{"protocol" => "ferricstore-native"}}
        0x0007 -> NativeServer.topology_payload(port)
        _opcode -> "OK"
      end
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _current -> port end)

    test_process = self()

    client_owner =
      spawn(fn ->
        result = SDK.start_link(seeds: [{"127.0.0.1", port}], heartbeat_interval: 5)
        send(test_process, {:sdk_started, result})

        receive do
          :close ->
            with {:ok, client} <- result, do: SDK.close(client)
        end
      end)

    assert_receive {:native_server_request, %{opcode: 0x000C}}, 100
    refute_receive {:native_server_request, %{opcode: 0x0003}}, 50

    assert_receive {:sdk_started, {:ok, client}}, 1_000

    on_exit(fn ->
      send(client_owner, :close)
      stop_server(server)
    end)

    assert_receive {:native_server_request, %{opcode: 0x0007}}, 100
    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0003}}, 150
  end

  test "an uncertain unsubscribe timeout resets the session and replays desired interests" do
    response_handler = fn
      %{opcode: 0x0012}, _port -> {:reply_after, 150, "OK"}
      _request, _port -> :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)
    assert {:ok, "OK"} = SDK.subscribe_events(client, ["flow_wake"])
    assert_receive {:native_server_request, %{opcode: 0x0011}}, 200
    old_connection = ClientRuntime.state(client).event_coordinator.subscriptions.connection

    assert {:error, :timeout} =
             SDK.unsubscribe_events(client, ["flow_wake"], call_timeout: 60)

    assert_eventually(fn -> not Process.alive?(old_connection) end)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}},
                   700

    assert ClientRuntime.state(client).event_coordinator.subscriptions.refcounts == %{
             "FLOW_WAKE" => 1
           }
  end

  test "removing an all-events subscription restores remaining specific interests" do
    {_server, client} = start_sdk()
    specific_subscriber = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      if Process.alive?(specific_subscriber), do: Process.exit(specific_subscriber, :kill)
    end)

    assert {:ok, "OK"} =
             SDK.subscribe_events(client, ["flow_wake"], subscriber: specific_subscriber)

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}}

    assert {:ok, "OK"} = SDK.subscribe_events(client, [])

    assert_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => @server_events}}}

    assert {:ok, "OK"} = SDK.unsubscribe_events(client, [])

    unowned_events = @server_events -- ["FLOW_WAKE", "TOPOLOGY_CHANGED"]

    assert_receive {:native_server_request,
                    %{opcode: 0x0012, payload: %{"events" => ^unowned_events}}}

    refute_receive {:native_server_request,
                    %{opcode: 0x0011, payload: %{"events" => ["FLOW_WAKE"]}}},
                   50
  end

  defp start_sdk(opts \\ []) do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    delay = Keyword.get(opts, :response_delay, 0)

    response_handler = Keyword.get(opts, :response_handler)

    response_fun = fn request ->
      port = Agent.get(port_holder, & &1)

      case response_handler && response_handler.(request, port) do
        nil -> default_response(request, port, delay)
        :default -> default_response(request, port, delay)
        response -> response
      end
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    client_opts =
      opts
      |> Keyword.take([
        :max_pending_requests,
        :max_event_subscribers,
        :max_event_queue,
        :heartbeat_interval,
        :connections_per_endpoint
      ])
      |> Keyword.put(:seeds, [{"127.0.0.1", port}])

    {:ok, client} = SDK.start_link(client_opts)

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    {server, client}
  end

  defp event_cancel_reductions(count) do
    response_handler = fn
      %{opcode: 0x0011, payload: %{"events" => ["TOPOLOGY_CHANGED"]}}, _port -> :default
      %{opcode: 0x0011}, _port -> :noreply
      _request, _port -> :default
    end

    {_server, client} =
      start_sdk(response_handler: response_handler, max_pending_requests: count * 3)

    active =
      spawn(fn ->
        SDK.subscribe_events(client, ["flow_wake"],
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_eventually(fn ->
      not is_nil(ClientRuntime.state(client).event_coordinator.operation)
    end)

    queued =
      Enum.map(1..count, fn _index ->
        spawn(fn ->
          SDK.subscribe_events(client, ["goaway"],
            timeout: :infinity,
            call_timeout: :infinity
          )
        end)
      end)

    assert_eventually(fn -> event_queue_size(client) == count end)
    {:reductions, before_reductions} = Process.info(client, :reductions)
    Enum.each(queued, &Process.exit(&1, :kill))
    assert_eventually(fn -> event_queue_size(client) == 0 end)
    {:reductions, after_reductions} = Process.info(client, :reductions)

    Process.exit(active, :kill)
    assert_eventually(fn -> is_nil(ClientRuntime.state(client).event_coordinator.operation) end)
    SDK.close(client)
    after_reductions - before_reductions
  end

  defp refresh_cancel_reductions(count) do
    {:ok, refreshes} = Agent.start_link(fn -> 0 end)

    response_handler = fn
      %{opcode: 0x0007}, port ->
        case Agent.get_and_update(refreshes, &{&1, &1 + 1}) do
          0 -> NativeServer.topology_payload(port)
          _later -> :noreply
        end

      _request, _port ->
        :default
    end

    {_server, client} = start_sdk(response_handler: response_handler)

    callers =
      Enum.map(1..count, fn _index ->
        spawn(fn -> SDK.refresh_topology(client) end)
      end)

    assert_eventually(fn -> refresh_waiter_count(client) == count end)
    {:reductions, before_reductions} = Process.info(client, :reductions)
    Enum.each(callers, &Process.exit(&1, :kill))

    assert_eventually(fn ->
      is_nil(ClientRuntime.state(client).topology_manager.refresh_operation)
    end)

    {:reductions, after_reductions} = Process.info(client, :reductions)
    SDK.close(client)
    after_reductions - before_reductions
  end

  defp event_queue_size(client) do
    case ClientRuntime.state(client).event_coordinator.queue do
      %{calls: calls} -> map_size(calls)
      queue -> :queue.len(queue)
    end
  end

  defp event_queue_calls(%{calls: calls}), do: Map.values(calls)
  defp event_queue_calls(queue), do: :queue.to_list(queue)

  defp refresh_waiter_count(client) do
    case ClientRuntime.state(client).topology_manager.refresh_operation do
      %{waiter_count: count} -> count
      %{waiters: waiters} -> length(waiters)
      nil -> 0
    end
  end

  defp start_sdk_with_slow_routed_endpoint(hello_delay) do
    data_response = fn
      %{opcode: 0x000C} -> {:reply_after, hello_delay, %{"protocol" => "ferricstore-native"}}
      _request -> "OK"
    end

    {:ok, data} = NativeServer.start_link(owner: self(), response_fun: data_response)
    data_port = NativeServer.port(data)

    seed_response = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(data_port, node: "data-node")
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, seed} = NativeServer.start_link(owner: self(), response_fun: seed_response)
    seed_endpoint = %{host: "127.0.0.1", native_port: NativeServer.port(seed)}
    {:ok, client} = SDK.start_link(seeds: [seed_endpoint])

    on_exit(fn -> stop_sdk(client, [seed, data]) end)

    {seed, data, client, seed_endpoint}
  end

  defp stop_sdk(client, servers) do
    SDK.close(client)
    Enum.each(servers, &stop_server/1)
  end

  defp stop_server(server) do
    if Process.alive?(server), do: GenServer.stop(server, :normal), else: :ok
  catch
    :exit, _reason -> :ok
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

  defp default_response(%{opcode: 0x0007}, port, _delay),
    do: NativeServer.topology_payload(port)

  defp default_response(%{opcode: 0x000C}, _port, _delay),
    do: %{"protocol" => "ferricstore-native"}

  defp default_response(%{opcode: opcode}, _port, delay) when opcode >= 0x0100 and delay > 0,
    do: {:reply_after, delay, "OK"}

  defp default_response(_request, _port, _delay), do: "OK"

  defp forward_events(parent) do
    receive do
      {:ferricstore_event, _client, event} ->
        send(parent, {:forwarded_event, self(), event})
        forward_events(parent)
    end
  end

  defp only_connection(client) do
    ClientRuntime.state(client)
    |> Map.fetch!(:connection_pool)
    |> Map.fetch!(:connections)
    |> Map.values()
    |> then(fn [connection] -> connection end)
  end

  defp pending_request_monitor_index_empty?(state) do
    Enum.all?(state.lifecycle_registry.owners, fn
      {_monitor, {:pending_request, _tag}} -> false
      _entry -> true
    end)
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
