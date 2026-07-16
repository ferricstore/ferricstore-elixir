defmodule FerricStore.ClientBoundaryTest do
  use ExUnit.Case, async: true

  alias FerricStore.AsyncRequest
  alias FerricStore.Client
  alias FerricStore.ClientIdentity
  alias FerricStore.Protocol.PipelineRequest
  alias FerricStore.SDK
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  setup do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)
    url = "ferric://127.0.0.1:#{port}"

    on_exit(fn ->
      stop_server(server)
    end)

    %{server: server, url: url}
  end

  test "all public client entry points expose supervisor child specs" do
    opts = [seeds: [{"127.0.0.1", 6_388}]]

    for module <- [FerricStore, FerricStore.Client, FerricStore.SDK] do
      assert %{
               id: ^module,
               start: {^module, :start_link, [^opts]},
               restart: :permanent,
               shutdown: :infinity,
               type: :supervisor
             } = module.child_spec(opts)
    end
  end

  test "public startup entry points return typed errors for malformed option containers" do
    malformed_options = [{:seeds, [{"127.0.0.1", 6_388}]} | :invalid_tail]

    starters = [
      &FerricStore.start_link/1,
      &Client.start_link/1,
      &SDK.start_link/1,
      &FerricStore.SDK.Native.Client.start_link/1
    ]

    for opts <- [malformed_options, [:not_a_keyword], :not_a_list], start <- starters do
      assert {:error, {:invalid_client_option, :options, ^opts}} = start.(opts)
    end

    assert {:error, {:invalid_client_option, :options, ^malformed_options}} =
             SDK.from_url("ferric://127.0.0.1:6388", malformed_options)
  end

  test "public startup entry points reject duplicate options before consuming them" do
    duplicate_tls = [seeds: [{"127.0.0.1", 1}], tls: false, tls: true]

    for start <- [
          &FerricStore.start_link/1,
          &Client.start_link/1,
          &SDK.start_link/1,
          &FerricStore.SDK.Native.Client.start_link/1
        ] do
      assert {:error, {:invalid_client_option, :options, {:duplicate_options, [:tls]}}} =
               start.(duplicate_tls)
    end

    assert {:error, {:invalid_client_option, :options, {:duplicate_options, [:url]}}} =
             SDK.start_link(url: "ferric://127.0.0.1:1", url: "ferric://127.0.0.1:2")

    assert {:error, {:invalid_client_option, :options, {:duplicate_options, [:username]}}} =
             SDK.from_url("ferric://127.0.0.1:1", username: "first", username: "second")
  end

  test "FerricStore starts the canonical topology-aware client", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)

    assert ClientIdentity.type(client) == :topology_aware
    assert %FerricStore.SDK.Native.Coordinator.State{} = ClientRuntime.state(client)
    assert {:ok, "OK"} = SDK.get(client, "shared-client")
    assert :ok = FerricStore.close(client)
  end

  test "both public facades accept the same client", %{server: server} do
    {:ok, client} = SDK.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])

    assert FerricStore.get(client, "shared-client") == "OK"
    assert FerricStore.command(client, "PING") == "OK"
    assert SDK.command(client, "PING") == {:ok, "OK"}
    assert :ok = Client.close(client)
  end

  test "public lifecycle and wait APIs reject unsafe timer values", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    unsafe_timeout = 1_000_000_000_000_000
    request = %AsyncRequest{client: client, source: client, ref: make_ref(), owner: self()}

    assert {:error, {:close_failed, {:invalid_timeout, ^unsafe_timeout}}} =
             Client.close(client, unsafe_timeout)

    assert {:error, %FerricStore.Error{raw: {:invalid_timeout, ^unsafe_timeout}}} =
             Client.await(request, unsafe_timeout)

    assert {:error, %FerricStore.Error{raw: {:invalid_timeout, ^unsafe_timeout}}} =
             Client.yield(request, unsafe_timeout)

    assert {:error, {:invalid_timeout, ^unsafe_timeout}} =
             SDK.await_event(client, unsafe_timeout)

    assert Process.alive?(client)
    assert :ok = Client.close(client)
  end

  test "high-level KV options use the canonical current native fields", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)

    assert :ok = FerricStore.set(client, "ttl-key", "value", ttl: 60_000)

    assert_receive {:native_server_request,
                    %{opcode: 0x0102, payload: %{"ttl" => 60_000} = set_payload}},
                   100

    refute Map.has_key?(set_payload, "ttl_ms")

    assert {:error,
            %FerricStore.Error{
              raw:
                {:invalid_kv_response, %{operation: :zrange, reason: :expected_member_score_list}}
            }} = FerricStore.zrange(client, "scores", 0, -1, withscores: true)

    assert_receive {:native_server_request,
                    %{opcode: 0x0142, payload: %{"withscores" => true} = zrange_payload}},
                   100

    refute Map.has_key?(zrange_payload, "with_scores")
    assert :ok = Client.close(client)
  end

  test "the high-level SET facade unwraps value-returning SET responses" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0102} -> "previous-value"
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:#{port}")

    on_exit(fn ->
      Client.close(client)
      stop_server(server)
    end)

    assert FerricStore.set(client, "key", "value", get: true) == "previous-value"
  end

  test "canonical asynchronous native calls use the topology client", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    request = Client.async_native(client, 0x0101, %{"key" => "async"})

    assert %{__struct__: FerricStore.AsyncRequest, client: ^client, owner: owner} = request
    assert owner == self()
    assert Client.await(request) == "OK"
    assert :ok = Client.close(client)
  end

  test "native request paths preserve false payloads instead of defaulting them", %{url: url} do
    {:ok, client} = SDK.from_url(url)

    assert {:ok, "OK"} = SDK.request(client, :ping, false)
    assert_receive {:native_server_request, %{payload: false}}, 100

    assert {:ok, "OK"} = SDK.request_by_key(client, :get, "false-payload-route", false)
    assert_receive {:native_server_request, %{payload: false}}, 100

    request = Client.async_native(client, :ping, false)
    assert Client.await(request) == "OK"
    assert_receive {:native_server_request, %{payload: false}}, 100

    assert :ok = Client.close(client)
  end

  test "an asynchronous call to a dead coordinator completes with a terminal error" do
    dead_client = spawn(fn -> :ok end)
    monitor = Process.monitor(dead_client)
    assert_receive {:DOWN, ^monitor, :process, ^dead_client, reason}
    assert reason in [:normal, :noproc]

    request =
      Client.async_native(dead_client, :get, %{"key" => "never-submitted"},
        call_timeout: :infinity
      )

    assert {:error, %FerricStore.Error{raw: :client_closed}} = Client.await(request, 100)
  end

  test "an admitted asynchronous call terminates when its coordinator is killed" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> NativeServer.startup_payload()
      %{opcode: 0x0101} -> :noreply
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _current -> port end)
    {:ok, client} = Client.start_link("ferric://127.0.0.1:#{port}")
    Process.unlink(client)

    on_exit(fn ->
      Client.close(client)
      stop_server(server)
    end)

    request = Client.async_native(client, :get, %{"key" => "orphaned"})

    assert map_size(ClientRuntime.state(client).request_registry.requests) == 1

    coordinator = ClientRuntime.coordinator(client)
    client_monitor = Process.monitor(client)
    Process.exit(coordinator, :kill)

    assert_receive {:DOWN, ^client_monitor, :process, ^client, _reason}, 500

    assert {:error, %FerricStore.Error{raw: :client_closed}} =
             Client.await(request, 100)
  end

  test "synchronous SDK calls to a dead coordinator return terminal errors instead of exiting" do
    dead_client = spawn(fn -> :ok end)
    monitor = Process.monitor(dead_client)
    assert_receive {:DOWN, ^monitor, :process, ^dead_client, reason}
    assert reason in [:normal, :noproc]

    assert SDK.get(dead_client, "key") == {:error, :client_closed}
    assert SDK.mget(dead_client, ["key"]) == {:error, :client_closed}
    assert SDK.subscribe_events(dead_client, ["flow_wake"]) == {:error, :client_closed}
    assert SDK.route(dead_client, "key") == {:error, :client_closed}
    assert SDK.refresh_topology(dead_client) == {:error, :client_closed}
    assert SDK.topology(dead_client) == {:error, :client_closed}
  end

  test "public native calls reject invalid explicit route hints", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    payload = %{"key" => "valid-payload-key"}

    assert {:error, %FerricStore.Error{raw: {:invalid_route_key, 123}}} =
             Client.native(client, :get, payload, route_key: 123)

    request = Client.async_native(client, :get, payload, route_key: 123)

    assert {:error, %FerricStore.Error{raw: {:invalid_route_key, 123}}} =
             Client.await(request)

    refute_receive {:native_server_request, %{opcode: 0x0101}}, 50
    assert :ok = Client.close(client)
  end

  test "routing facades return canonical errors for malformed request options" do
    malformed_options = [{:route_key, "route"} | :invalid_tail]

    for opts <- [malformed_options, :not_a_keyword] do
      assert {:error, %FerricStore.Error{raw: {:invalid_request_option, :options, ^opts}}} =
               Client.native(self(), :get, %{"key" => "key"}, opts)

      assert {:error, %FerricStore.Error{raw: {:invalid_request_option, :options, ^opts}}} =
               Client.command(self(), "PING", [], opts)
    end
  end

  test "the high-level raw-command facade rejects scalar argument containers" do
    assert {:error, %FerricStore.Error{raw: {:invalid_command_args, :expected_list}}} =
             Client.command(self(), "GET", :not_a_list)

    assert {:error, %FerricStore.Error{raw: {:invalid_command_args, :expected_list}}} =
             FerricStore.command(self(), "GET", :not_a_list)
  end

  test "routing facades reject duplicate route options before consuming them" do
    for {duplicate, opts} <- [
          {:route_key, [route_key: "first", route_key: "second"]},
          {:key, [key: "first", key: "second"]}
        ] do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_request_option, :options, {:duplicate_options, [^duplicate]}}
              }} = Client.native(self(), :get, %{"key" => "payload-key"}, opts)
    end
  end

  test "routing facades reject conflicting route option names" do
    opts = [route_key: "explicit-route", key: "command-route"]

    for call <- [
          fn -> Client.native(self(), :get, %{"key" => "payload-key"}, opts) end,
          fn -> Client.command(self(), "GET", ["payload-key"], opts) end
        ] do
      assert {:error,
              %FerricStore.Error{
                raw: {:conflicting_route_options, [:key, :route_key]}
              }} = call.()
    end
  end

  test "all high-level facades use the same structured error contract" do
    assert {:error, %FerricStore.Error{raw: {:invalid_route_key, :not_binary}}} =
             FerricStore.get(self(), :not_binary)

    assert {:error, %FerricStore.Error{raw: {:invalid_route_key, :not_binary}}} =
             Client.native(self(), :get, %{"key" => "valid"}, route_key: :not_binary)
  end

  test "public native routing establishes the deadline before scanning route lists" do
    payload = %{"partition_keys" => List.duplicate("tenant-a", 100_000)}
    :erlang.garbage_collect(self())
    {:reductions, before_sync} = Process.info(self(), :reductions)

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Client.native(self(), :flow_claim_due, payload, timeout: 0)

    {:reductions, after_sync} = Process.info(self(), :reductions)
    assert after_sync - before_sync < 100_000

    :erlang.garbage_collect(self())
    {:reductions, before_async} = Process.info(self(), :reductions)
    request = Client.async_native(self(), :flow_claim_due, payload, timeout: 0)
    {:reductions, after_async} = Process.info(self(), :reductions)

    assert after_async - before_async < 100_000
    assert {:error, %FerricStore.Error{raw: :timeout}} = Client.await(request)
  end

  test "an async await timeout cancels the request and consumes any racing reply" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0101} -> {:reply_after, 150, "late"}
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:#{port}")

    on_exit(fn ->
      Client.close(client)
      stop_server(server)
    end)

    request = Client.async_native(client, 0x0101, %{"key" => "async"}, key: "async")

    assert {:error, %FerricStore.Error{raw: :timeout}} = Client.await(request, 5)
    assert ClientRuntime.state(client).request_registry.requests == %{}
    ref = request.ref
    refute_receive {FerricStore.AsyncRequest, ^ref, _result}, 250
  end

  test "an await timeout is not extended by an unresponsive coordinator", %{url: url} do
    {:ok, client} = Client.start_link(url)
    :ok = ClientRuntime.suspend(client)

    request = %AsyncRequest{
      client: client,
      source: ClientRuntime.coordinator(client),
      ref: make_ref(),
      owner: self()
    }

    started = System.monotonic_time(:millisecond)

    try do
      assert {:error, %FerricStore.Error{raw: :timeout}} = Client.await(request, 1)
      assert System.monotonic_time(:millisecond) - started < 100
    after
      :ok = ClientRuntime.resume(client)
      Client.close(client)
    end
  end

  test "an await timeout drops a response already queued behind cancellation" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x000C} -> %{"protocol" => "ferricstore-native"}
      %{opcode: 0x0101} -> {:reply_after, 40, "late-success"}
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = Client.start_link("ferric://127.0.0.1:#{port}")

    on_exit(fn ->
      Client.close(client)
      stop_server(server)
    end)

    request = Client.async_native(client, :get, %{"key" => "queued-late"})

    assert_receive {:native_server_request, %{opcode: 0x0101}}, 200
    :ok = ClientRuntime.suspend(client)
    Process.sleep(80)

    assert {:error, %FerricStore.Error{raw: :timeout}} = Client.await(request, 0)
    :ok = ClientRuntime.resume(client)

    ref = request.ref
    refute_receive {FerricStore.AsyncRequest, ^ref, _result}, 150
  end

  test "oversized pipelines are rejected before entering the coordinator", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    :ok = ClientRuntime.suspend(client)
    commands = List.duplicate(["PING"], 1_000_000)

    request =
      Task.async(fn ->
        try do
          Client.pipeline(client, commands, call_timeout: 50)
        catch
          :exit, {:timeout, _call} -> :entered_coordinator
        end
      end)

    try do
      assert {:error,
              %FerricStore.Error{
                raw: {:pipeline_too_large, %{items: 100_001, limit: 100_000}}
              }} = Task.await(request, 250)
    after
      :ok = ClientRuntime.resume(client)
      Task.shutdown(request, :brutal_kill)
      Client.close(client)
    end
  end

  test "pipeline facades return typed errors for non-list commands", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)

    assert {:error, %FerricStore.Error{raw: {:invalid_pipeline, :expected_list}}} =
             Client.pipeline(client, :not_a_list)

    request = Client.async_pipeline(client, :not_a_list)

    assert {:error, %FerricStore.Error{raw: {:invalid_pipeline, :expected_list}}} =
             Client.await(request)

    refute_receive {:native_server_request, %{opcode: 0x000E}}, 50
    assert :ok = Client.close(client)
  end

  test "malformed pipeline entries are rejected before entering the coordinator", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    :ok = ClientRuntime.suspend(client)

    try do
      assert {:error,
              %FerricStore.Error{
                raw:
                  {:invalid_pipeline_command,
                   %{index: 0, reason: :expected_nonempty_list_or_typed_map}}
              }} = Client.pipeline(client, ["PING"], call_timeout: 50)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_pipeline_command, %{index: 0, reason: :invalid_command_arguments}}
              }} =
               Client.pipeline(client, [["PING", "arg" | :invalid_tail]], call_timeout: 50)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_pipeline_command, %{index: 0, reason: :unsupported_fields}}
              }} =
               Client.pipeline(
                 client,
                 [%{opcode: FerricStore.Protocol.opcode(:get), body: %{}, typo: true}],
                 call_timeout: 50
               )

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_pipeline_command, %{index: 0, reason: :control_opcode}}
              }} =
               Client.pipeline(
                 client,
                 [%{opcode: FerricStore.Protocol.opcode(:hello), body: %{}}],
                 call_timeout: 50
               )
    after
      :ok = ClientRuntime.resume(client)
      Client.close(client)
    end
  end

  test "pipeline facades return typed errors for malformed options" do
    malformed_options = [{:return, :values} | :invalid_tail]

    for opts <- [malformed_options, :not_a_keyword] do
      assert {:error, %FerricStore.Error{raw: {:invalid_request_option, :options, ^opts}}} =
               Client.pipeline(self(), [], opts)

      request = Client.async_pipeline(self(), [], opts)

      assert {:error, %FerricStore.Error{raw: {:invalid_request_option, :options, ^opts}}} =
               Client.await(request)
    end
  end

  test "pipeline return modes reject unsupported values instead of being discarded" do
    for value <- [:values, "values", true] do
      assert {:error, %FerricStore.Error{raw: {:invalid_pipeline_option, :return, ^value}}} =
               Client.pipeline(self(), [], return: value)

      request = Client.async_pipeline(self(), [], return: value)

      assert {:error, %FerricStore.Error{raw: {:invalid_pipeline_option, :return, ^value}}} =
               Client.await(request)
    end
  end

  test "public pipeline facades reject options that have no pipeline effect" do
    for {key, value} <- [
          max_group_concurrency: 2,
          key: "ignored-route",
          route_key: "ignored-route",
          unknown: :value
        ] do
      expected = {:invalid_pipeline_option, key, value}

      assert {:error, %FerricStore.Error{raw: ^expected}} =
               Client.pipeline(self(), [], [{key, value}])

      request = Client.async_pipeline(self(), [], [{key, value}])
      assert {:error, %FerricStore.Error{raw: ^expected}} = Client.await(request)
    end
  end

  test "native pipeline facade rejects malformed option containers without raising" do
    native_client = FerricStore.SDK.Native.Client

    cases = [
      {:not_pipeline_options, [], {:invalid_pipeline_option, :options, :not_pipeline_options}},
      {[], :not_request_options, {:invalid_request_option, :options, :not_request_options}}
    ]

    for {pipeline_options, request_options, expected} <- cases do
      assert {:error, ^expected} =
               native_client.pipeline(self(), [], pipeline_options, request_options)

      ref = native_client.async_pipeline(self(), [], pipeline_options, request_options)
      assert_receive {FerricStore.AsyncRequest, ^ref, {:error, ^expected}}
    end
  end

  test "native pipeline facade rejects options that would otherwise be ignored" do
    native_client = FerricStore.SDK.Native.Client

    for {key, value} <- [
          timeout: 10,
          lane_id: 1,
          idempotent: true,
          max_group_concurrency: 2,
          unknown: :value
        ] do
      expected = {:invalid_pipeline_option, key, value}

      assert {:error, ^expected} = native_client.pipeline(self(), [], [{key, value}], [])

      ref = native_client.async_pipeline(self(), [], [{key, value}], [])
      assert_receive {FerricStore.AsyncRequest, ^ref, {:error, ^expected}}
    end
  end

  test "native pipeline request options reject fields with no transport effect" do
    native_client = FerricStore.SDK.Native.Client

    for {key, value} <- [unknown: :value, endpoint: %{host: "ignored"}, request_context: %{}] do
      expected = {:invalid_request_option, key, value}

      assert {:error, ^expected} = native_client.pipeline(self(), [], [], [{key, value}])

      ref = native_client.async_pipeline(self(), [], [], [{key, value}])
      assert_receive {FerricStore.AsyncRequest, ^ref, {:error, ^expected}}
    end
  end

  test "pipelines honor the server-negotiated command limit before encoding" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x000C} ->
        %{
          "protocol" => "ferricstore-native",
          "capabilities" => %{"limits" => %{"max_pipeline_commands" => 2}}
        }

      %{opcode: 0x000E} ->
        []

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = Client.start_link("ferric://127.0.0.1:#{port}")

    on_exit(fn ->
      Client.close(client)
      stop_server(server)
    end)

    forged_pipeline = %PipelineRequest{
      commands: [["PING"], ["PING"], ["PING"]],
      command_count: 1
    }

    assert {:error, {:invalid_request_payload, %{reason: :reserved_pipeline_envelope}}} =
             SDK.request(client, :pipeline, forged_pipeline)

    refute_receive {:native_server_request, %{opcode: 0x000E}}, 50

    assert {:error, %FerricStore.Error{raw: {:pipeline_too_large, %{items: 3, limit: 2}}}} =
             Client.pipeline(client, [["PING"], ["PING"], ["PING"]])

    refute_receive {:native_server_request, %{opcode: 0x000E}}, 50
    assert [] = Client.pipeline(client, [["PING"], ["PING"]])
    assert_receive {:native_server_request, %{opcode: 0x000E}}, 100
  end

  test "finite flow batch writes carry a server-visible deadline", %{url: url} do
    {:ok, client} = Client.start_link(url)
    before_ms = System.system_time(:millisecond)

    assert "OK" =
             FerricStore.Flow.create_many(client, ["deadline-flow"],
               type: "deadline-test",
               now_ms: 10,
               timeout: 250
             )

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x020F,
                      payload: %{"deadline_ms" => deadline_ms, "items" => [["deadline-flow", ""]]}
                    }},
                   100

    assert deadline_ms >= before_ms
    assert deadline_ms <= System.system_time(:millisecond) + 250
    assert :ok = Client.close(client)
  end

  test "the high-level mset facade bounds input before pair normalization" do
    pairs = List.duplicate({"key", "value"}, 100_001) ++ [:invalid_tail]

    assert {:error,
            %FerricStore.Error{
              raw: {:batch_too_large, %{items: 100_001, limit: 100_000}}
            }} =
             FerricStore.mset(self(), pairs)
  end

  test "the high-level mset facade returns structured errors for invalid containers" do
    assert {:error, %FerricStore.Error{raw: {:invalid_mset_pairs, :invalid}}} =
             FerricStore.mset(self(), :invalid)
  end

  test "close cannot terminate an unrelated process" do
    {:ok, unrelated} = Agent.start_link(fn -> :important_state end)

    on_exit(fn ->
      if Process.alive?(unrelated), do: Agent.stop(unrelated)
    end)

    assert Client.close(unrelated) == {:error, {:invalid_client, :unknown}}
    assert Process.alive?(unrelated)
    assert Agent.get(unrelated, & &1) == :important_state
  end

  test "all public close facades return typed errors for malformed client handles" do
    for close <- [&FerricStore.close/1, &Client.close/1, &SDK.close/1] do
      assert {:error, {:invalid_client, :not_pid}} = close.(:not_a_client)
    end

    assert {:error, {:invalid_client, :not_pid}} = Client.close(%{}, 10)
  end

  test "public SDK calls return typed errors for malformed client handles" do
    invalid_client = :not_a_client
    expected = {:error, {:client_unavailable, :invalid_client}}

    calls = [
      fn -> SDK.topology(invalid_client) end,
      fn -> SDK.route(invalid_client, "key") end,
      fn -> SDK.refresh_topology(invalid_client) end,
      fn -> SDK.ping(invalid_client) end,
      fn -> SDK.get(invalid_client, "key") end,
      fn -> SDK.mget(invalid_client, ["key"]) end,
      fn -> SDK.del(invalid_client, ["key"]) end,
      fn -> SDK.mset(invalid_client, [{"key", "value"}]) end,
      fn -> SDK.await_event(invalid_client, 0) end
    ]

    Enum.each(calls, fn call -> assert call.() == expected end)
  end

  test "event waiting rejects unrelated live pids without accepting forged events" do
    {:ok, unrelated} = Agent.start_link(fn -> :unrelated end)
    forged = %{event: "forged"}
    send(self(), {:ferricstore_event, unrelated, forged})

    assert SDK.await_event(unrelated, 0) ==
             {:error, {:client_unavailable, :invalid_client}}

    assert_receive {:ferricstore_event, ^unrelated, ^forged}
  end

  test "async lifecycle APIs reject malformed handles without raising" do
    assert {:error, %FerricStore.Error{raw: {:invalid_async_request, :expected_handle}}} =
             Client.await(:not_a_request, 0)

    assert {:error, %FerricStore.Error{raw: {:invalid_async_request, :expected_handle}}} =
             Client.yield(:not_a_request, 0)

    assert {:error, {:invalid_async_request, :expected_handle}} =
             Client.cancel_async(:not_a_request, 0)

    invalid_ref = %AsyncRequest{
      client: self(),
      source: self(),
      ref: :not_a_reference,
      owner: self()
    }

    assert {:error, %FerricStore.Error{raw: {:invalid_async_request, :invalid_reference}}} =
             Client.await(invalid_ref, 0)

    assert {:error, %FerricStore.Error{raw: {:invalid_async_request, :invalid_reference}}} =
             Client.yield(invalid_ref, 0)

    assert {:error, {:invalid_async_request, :invalid_reference}} =
             Client.cancel_async(invalid_ref, 0)

    invalid_client = %AsyncRequest{
      client: :not_a_client,
      source: self(),
      ref: make_ref(),
      owner: self()
    }

    assert {:error, {:invalid_async_request, :invalid_client}} =
             Client.cancel_async(invalid_client, 0)
  end

  test "legacy process labels are not accepted as client identities" do
    test_pid = self()

    impostor =
      spawn(fn ->
        Process.set_label({ClientIdentity, :topology_aware})
        send(test_pid, {:impostor_ready, self()})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(impostor), do: Process.exit(impostor, :kill) end)
    assert_receive {:impostor_ready, ^impostor}

    assert ClientIdentity.type(impostor) == :unknown
    assert Client.close(impostor) == {:error, {:invalid_client, :unknown}}
    assert Process.alive?(impostor)
  end

  test "current client labels cannot impersonate the registered supervisor", %{url: url} do
    {:ok, client} = FerricStore.start_link(url: url)
    {:ok, endpoint} = ClientIdentity.endpoint(client)
    test_pid = self()

    impostor =
      spawn(fn ->
        ClientIdentity.mark(:topology_aware, endpoint)
        send(test_pid, {:impostor_ready, self()})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      if Process.alive?(impostor), do: Process.exit(impostor, :kill)
      if Process.alive?(client), do: FerricStore.close(client)
    end)

    assert_receive {:impostor_ready, ^impostor}
    assert ClientIdentity.type(impostor) == :unknown
    assert SDK.get(impostor, "key") == {:error, {:client_unavailable, :invalid_client}}
    assert Process.alive?(impostor)
  end

  test "close reports a stop timeout instead of claiming success" do
    test_pid = self()

    unresponsive_client =
      spawn(fn ->
        endpoint = :ets.new(__MODULE__, [:set, :public])
        true = :ets.insert(endpoint, {:client, self()})
        ClientIdentity.mark(:topology_aware, endpoint)
        send(test_pid, {:unresponsive_client_ready, self()})

        receive do
          :stop -> :ok
        end
      end)

    on_exit(fn ->
      if Process.alive?(unresponsive_client), do: Process.exit(unresponsive_client, :kill)
    end)

    assert_receive {:unresponsive_client_ready, ^unresponsive_client}
    assert Client.close(unresponsive_client, 10) == {:error, {:close_failed, :timeout}}
    assert Process.alive?(unresponsive_client)
  end

  defp stop_server(server) do
    if Process.alive?(server), do: GenServer.stop(server, :normal)
  catch
    :exit, _reason -> :ok
  end
end
