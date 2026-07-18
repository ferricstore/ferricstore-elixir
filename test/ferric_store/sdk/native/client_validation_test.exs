defmodule FerricStore.SDK.Native.ClientValidationTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{PipelineRequest, PreparedMap}
  alias FerricStore.SDK
  alias FerricStore.SDK.Native.Client, as: NativeClient
  alias FerricStore.SDK.Native.ClientOptions
  alias FerricStore.SDK.Native.Topology
  alias FerricStore.Test.{ClientRuntime, NativeServer}
  alias FerricStore.Transport.CACerts

  test "direct native cancellation rejects malformed request identities without raising" do
    ref = make_ref()

    assert {:error, {:cancel_failed, {:invalid_async_request, :invalid_client}}} =
             NativeClient.cancel_async(:not_a_client, self(), ref, 0)

    assert {:error, {:cancel_failed, {:invalid_async_request, :invalid_owner}}} =
             NativeClient.cancel_async(self(), :not_an_owner, ref, 0)

    assert {:error, {:cancel_failed, {:invalid_async_request, :invalid_reference}}} =
             NativeClient.cancel_async(self(), self(), :not_a_reference, 0)
  end

  test "invalid route keys return errors without terminating the shared client" do
    {_server, client} = start_sdk()

    assert {:error, {:invalid_route_key, :not_binary}} = SDK.get(client, :not_binary)
    assert {:error, {:invalid_route_key, :not_binary}} = SDK.route(client, :not_binary)

    assert {:error, {:invalid_route_key, :not_binary}} =
             SDK.command_exec(client, "GET", ["valid-payload-key"], key: :not_binary)

    refute_receive {:native_server_request, %{opcode: 0x0100}}, 50
    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.get(client, "valid-key")
  end

  test "raw commands reject invalid routing before scanning large argument lists" do
    args = List.duplicate("argument", 100_000)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, {:invalid_route_key, :not_binary}} =
             SDK.command_exec(self(), "GET", args, key: :not_binary)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 20_000
  end

  test "invalid per-request timeouts fail locally without terminating the client" do
    {_server, client} = start_sdk()

    invalid_options = [
      {:timeout, -1},
      {:call_timeout, -1},
      {:timeout, 1_000_000_000_000_000},
      {:call_timeout, 1_000_000_000_000_000}
    ]

    Enum.each(invalid_options, fn {key, value} ->
      assert {:error, {:invalid_request_option, ^key, ^value}} =
               SDK.get(client, "invalid-timeout", [{key, value}])

      refute_receive {:native_server_request, %{opcode: 0x0101}}, 25
      assert Process.alive?(client)
    end)

    assert {:ok, "OK"} = SDK.get(client, "valid-timeout")
  end

  test "duplicate request options fail locally instead of selecting the first value" do
    {_server, client} = start_sdk()

    cases = [
      {[timeout: :infinity, timeout: 0], [:timeout]},
      {[idempotent: true, lane_id: 1, idempotent: false, lane_id: 2], [:idempotent, :lane_id]}
    ]

    Enum.each(cases, fn {opts, duplicates} ->
      assert {:error, {:invalid_request_option, :options, {:duplicate_options, ^duplicates}}} =
               SDK.request(client, :get, %{"key" => "ambiguous-options"}, opts)

      refute_receive {:native_server_request, %{opcode: 0x0101}}, 25
      assert Process.alive?(client)
    end)
  end

  test "unknown native request options are rejected instead of being silently ignored" do
    {_server, client} = start_sdk()
    option = {:timout, 25}
    expected = {:error, {:invalid_request_option, :timout, 25}}

    calls = [
      fn -> SDK.request(client, :get, %{"key" => "unknown-request-option"}, [option]) end,
      fn -> SDK.command_exec(client, "PING", [], [option]) end,
      fn -> SDK.subscribe_events(client, ["FLOW_WAKE"], [option]) end
    ]

    Enum.each(calls, fn call -> assert call.() == expected end)

    for opcode <- [0x0011, 0x0100, 0x0101] do
      refute_receive {:native_server_request, %{opcode: ^opcode}}, 25
    end
  end

  test "recognized request controls are rejected on surfaces where they have no effect" do
    {_server, client} = start_sdk()

    cases = [
      {
        fn ->
          SDK.request(client, :ping, %{}, request_context: %{subject: "ignored"})
        end,
        {:invalid_request_option, :request_context, %{subject: "ignored"}}
      },
      {
        fn -> SDK.request_by_key(client, :get, "key", %{"key" => "key"}, lane_id: 7) end,
        {:invalid_request_option, :lane_id, 7}
      },
      {
        fn -> SDK.command_exec(client, "PING", [], max_group_concurrency: 2) end,
        {:invalid_request_option, :max_group_concurrency, 2}
      },
      {
        fn ->
          SDK.request_by_items(
            client,
            :mget,
            ["key"],
            & &1,
            &%{"keys" => &1},
            lane_id: 9
          )
        end,
        {:invalid_request_option, :lane_id, 9}
      },
      {
        fn -> SDK.command_exec(client, "GET", ["key"], key: "key", endpoint: %{}) end,
        {:invalid_request_option, :endpoint, %{}}
      }
    ]

    Enum.each(cases, fn {call, expected} -> assert call.() == {:error, expected} end)

    for opcode <- [0x0003, 0x0100, 0x0101, 0x0104] do
      refute_receive {:native_server_request, %{opcode: ^opcode}}, 25
    end
  end

  test "event subscriptions return typed errors for malformed request options" do
    malformed_options = [{:subscriber, self()} | :invalid_tail]

    for opts <- [malformed_options, :not_a_keyword] do
      assert {:error, {:invalid_request_option, :options, ^opts}} =
               SDK.subscribe_events(self(), ["FLOW_WAKE"], opts)

      assert {:error, {:invalid_request_option, :options, ^opts}} =
               SDK.unsubscribe_events(self(), ["FLOW_WAKE"], opts)
    end
  end

  test "event subscriptions reject malformed event lists without crashing the client" do
    {_server, client} = start_sdk()
    improper_events = ["FLOW_WAKE" | :invalid_tail]

    assert {:error, {:invalid_event_list, :improper_list}} =
             SDK.subscribe_events(client, improper_events)

    assert {:error, {:invalid_event_list, :expected_list}} =
             SDK.unsubscribe_events(client, :not_a_list)

    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.get(client, "still-alive")
  end

  test "event subscriptions reject malformed filter identifiers before wire work" do
    {_server, client} = start_sdk()

    for invalid_filter <- [123, nil, false, true],
        call <- [
          fn -> SDK.subscribe_events(client, ["FLOW_WAKE", invalid_filter]) end,
          fn -> SDK.unsubscribe_events(client, ["FLOW_WAKE", invalid_filter]) end
        ] do
      assert {:error,
              {:invalid_event_filter,
               %{
                 index: 1,
                 reason: :expected_nonempty_binary_or_atom,
                 value: ^invalid_filter
               }}} = call.()
    end

    for opcode <- [0x0011, 0x0012] do
      refute_receive {:native_server_request, %{opcode: ^opcode}}, 25
    end

    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.get(client, "still-alive-after-invalid-filter")
  end

  test "event subscriptions reject invalid UTF-8 before coordinator normalization" do
    {_server, client} = start_sdk()
    invalid_event = <<0xFF>>

    assert {:error,
            {:invalid_event_filter, %{index: 0, reason: :invalid_utf8, value: ^invalid_event}}} =
             SDK.subscribe_events(client, [invalid_event])

    refute_receive {:native_server_request, %{opcode: 0x0011}}, 50
    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.get(client, "still-alive-after-invalid-utf8")
  end

  test "event subscriptions reject unsupported and oversized filters locally" do
    {_server, client} = start_sdk()

    for {event, reason} <- [
          {"NOT_SUPPORTED", :unsupported_event},
          {String.duplicate("x", 129), :identifier_too_long}
        ] do
      assert {:error, {:invalid_event_filter, %{index: 0, reason: ^reason, value: ^event}}} =
               SDK.subscribe_events(client, [event])
    end

    refute_receive {:native_server_request, %{opcode: 0x0011}}, 50
    assert Process.alive?(client)
  end

  test "typed request options fail locally before retry policy can crash the client" do
    {_server, client} = start_sdk()

    invalid_options = [
      {:idempotent, :yes},
      {:max_group_concurrency, 0},
      {:lane_id, -1},
      {:request_context, "not-a-context"}
    ]

    Enum.each(invalid_options, fn {key, value} ->
      assert {:error, {:invalid_request_option, ^key, ^value}} =
               SDK.get(client, "invalid-request-option", [{key, value}])

      refute_receive {:native_server_request, %{opcode: 0x0101}}, 25
      assert Process.alive?(client)
    end)

    assert {:ok, "OK"} = SDK.get(client, "valid-request-options", idempotent: true)
  end

  test "command request contexts reject malformed identity fields before wire work" do
    {_server, client} = start_sdk()

    for {request_context, field} <- [
          {%{subject: 123}, "subject"},
          {%{tenant: false}, "tenant"},
          {%{scopes: %{admin: true}}, "scopes"}
        ] do
      assert {:error, {:invalid_request_context, message}} =
               SDK.command_exec(client, "PING", [], request_context: request_context)

      assert message =~ "request context field #{field}"
    end

    refute_receive {:native_server_request, %{opcode: 0x0100}}, 50
    assert Process.alive?(client)
  end

  test "a failing public key callback cannot crash the coordinator" do
    {_server, client} = start_sdk()

    assert {:error, {:route_key_failed, "bad key callback"}} =
             SDK.request_by_items(
               client,
               :mget,
               ["key"],
               fn _item -> raise "bad key callback" end,
               &%{"keys" => &1}
             )

    assert Process.alive?(client)
  end

  test "public batch request boundaries return typed errors for malformed arguments" do
    {_server, client} = start_sdk()

    assert {:error, {:invalid_batch_items, :expected_list}} =
             SDK.request_by_keys(client, :mget, :not_a_list, &%{"keys" => &1})

    assert {:error, {:invalid_batch_callback, :payload_builder}} =
             SDK.request_by_keys(client, :mget, ["key"], :not_a_callback)

    assert {:error, {:invalid_batch_callback, :key_fun}} =
             SDK.request_by_items(client, :mget, ["key"], :not_a_callback, &%{"keys" => &1})

    assert {:error, {:invalid_batch_callback, :payload_builder}} =
             SDK.request_by_items(client, :mget, ["key"], & &1, :not_a_callback)

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 25
    assert Process.alive?(client)
  end

  test "public batch payload builders must return maps" do
    {_server, client} = start_sdk()

    for invalid_payload <- [nil, false] do
      assert {:error, {:invalid_batch_payload, ^invalid_payload}} =
               SDK.request_by_keys(
                 client,
                 :mget,
                 ["key"],
                 fn _keys -> invalid_payload end
               )
    end

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 50
    assert Process.alive?(client)
  end

  test "public batch payload builders cannot forge trusted protocol envelopes" do
    {_server, client} = start_sdk()
    {:ok, prepared} = PreparedMap.prepare(%{"keys" => ["key"]}, 1_024)

    reserved_payloads = [
      {%PipelineRequest{commands: [], command_count: 0}, :reserved_pipeline_envelope},
      {prepared, :reserved_prepared_map}
    ]

    for {reserved_payload, reason} <- reserved_payloads do
      assert {:error, {:invalid_batch_payload, %{reason: ^reason}}} =
               SDK.request_by_keys(client, :mget, ["key"], fn _keys -> reserved_payload end)
    end

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 50
    assert Process.alive?(client)
  end

  test "public requests cannot forge internal batch cardinality envelopes" do
    {_server, client} = start_sdk()
    reserved_payload = {:custom_payload, <<0x89, 0::32, 0::32>>, {:batch_items, 0}}

    assert {:error, {:invalid_request_payload, %{reason: :reserved_batch_envelope}}} =
             SDK.request(client, :mget, reserved_payload)

    assert {:error, {:invalid_batch_payload, %{reason: :reserved_batch_envelope}}} =
             SDK.request_by_keys(client, :mget, ["key"], fn _keys -> reserved_payload end)

    refute_receive {:native_server_request, %{opcode: 0x0104}}, 50
    assert Process.alive?(client)
  end

  test "client inspection redacts the authentication password" do
    {_server, client} =
      start_sdk(username: "service", password: "audit-password-must-not-appear")

    inspected = ClientRuntime.state(client) |> inspect(limit: :infinity)

    refute inspected =~ "audit-password-must-not-appear"
    assert inspected =~ "FerricStore.SDK.Native.Client"
  end

  test "out-of-range and unsupported generic protocol values return encode errors" do
    {_server, client} = start_sdk()

    assert {:error, {:encode_failed, integer_message}} =
             SDK.request(client, :ping, %{"message" => 9_223_372_036_854_775_808})

    assert integer_message =~ "signed 64-bit"

    assert {:error, {:encode_failed, type_message}} =
             SDK.request(client, :ping, %{"message" => self()})

    assert type_message =~ "cannot encode"
    assert Process.alive?(client)
  end

  test "oversized outbound collections fail locally without reaching the server" do
    {_server, client} = start_sdk()
    values = List.duplicate(nil, 100_001)

    assert {:error, {:encode_failed, message}} =
             SDK.request(client, :ping, %{"message" => values})

    assert message =~ "collection exceeds 100000 items"
    refute_receive {:native_server_request, %{opcode: 0x0102}}, 50
    assert Process.alive?(client)
  end

  test "oversized command arguments are rejected before entering the coordinator" do
    {_server, client} = start_sdk()
    :ok = ClientRuntime.suspend(client)
    args = List.duplicate("too-large", 1_000_000)

    request =
      Task.async(fn ->
        try do
          SDK.command_exec(client, "ECHO", args, call_timeout: 50)
        catch
          :exit, {:timeout, _call} -> :entered_coordinator
        end
      end)

    try do
      assert Task.await(request, 250) ==
               {:error, {:command_too_large, %{items: 100_001, limit: 100_000}}}
    after
      :ok = ClientRuntime.resume(client)
      Task.shutdown(request, :brutal_kill)
    end
  end

  test "raw command execution rejects malformed names and argument containers locally" do
    {_server, client} = start_sdk()

    assert {:error, {:invalid_command_args, :expected_list}} =
             SDK.command_exec(client, "GET", :not_a_list)

    for {command, reason} <- [
          {:get, :expected_binary},
          {"", :empty},
          {<<0xFF>>, :invalid_utf8},
          {String.duplicate("x", 1_025), :too_long}
        ] do
      assert {:error, {:invalid_command, %{reason: ^reason, value: ^command}}} =
               SDK.command_exec(client, command, ["key"])
    end

    refute_receive {:native_server_request, %{opcode: 0x0100}}, 25
    assert Process.alive?(client)
  end

  test "malformed per-request endpoints return errors without crashing the shared client" do
    {_server, client} = start_sdk()
    Process.unlink(ClientRuntime.state(client).runtime_supervisor)

    assert {:error, {:invalid_endpoint, "not-an-endpoint"}} =
             SDK.ping(client, "ping", endpoint: "not-an-endpoint")

    assert {:error, {:invalid_endpoint, %{host: "127.0.0.1", native_port: 70_000}}} =
             SDK.ping(client, "ping", endpoint: %{host: "127.0.0.1", native_port: 70_000})

    conflicting = %{
      :host => "127.0.0.1",
      "host" => "other.invalid",
      :native_port => 6_388
    }

    assert {:error, {:invalid_endpoint, ^conflicting}} =
             SDK.ping(client, "ping", endpoint: conflicting)

    assert Process.alive?(client)
    assert {:ok, "OK"} = SDK.ping(client)
  end

  test "malformed TLS endpoint options return errors without crashing the shared client" do
    {server, client} = start_sdk()
    port = NativeServer.port(server)
    Process.unlink(ClientRuntime.state(client).runtime_supervisor)

    invalid_endpoints = [
      %{
        host: "127.0.0.1",
        native_port: port,
        native_tls_port: port,
        tls: true,
        cacerts: :invalid
      },
      %{
        host: "127.0.0.1",
        native_port: port,
        native_tls_port: port,
        tls: true,
        cacertfile: 123
      },
      %{
        host: "127.0.0.1",
        native_port: port,
        native_tls_port: port,
        tls: true,
        server_name: 123
      }
    ]

    Enum.each(invalid_endpoints, fn endpoint ->
      assert {:error, {:invalid_endpoint, ^endpoint}} =
               SDK.ping(client, "invalid-tls-option", endpoint: endpoint)

      assert Process.alive?(client)
    end)

    assert {:ok, "OK"} = SDK.ping(client)
  end

  test "malformed startup endpoint options are rejected before client initialization" do
    invalid_options = [
      {:server_name, 123},
      {:cacertfile, 123},
      {:cacertfile, [0x11_0000]},
      {:cacerts, :invalid},
      {:verify, :yes},
      {:max_response_bytes, 0},
      {:heartbeat_interval, 0},
      {:heartbeat_timeout, -1},
      {:send_timeout, :infinity},
      {:drain_timeout, :infinity},
      {:connect_timeout, 1_000_000_000_000_000},
      {:server_chunk_timeout, 1_000_000_000_000_000},
      {:heartbeat_interval, 1_000_000_000_000_000},
      {:heartbeat_timeout, 1_000_000_000_000_000},
      {:drain_timeout, 1_000_000_000_000_000},
      {:tls, :yes},
      {:warm_connections, :yes},
      {:username, ""},
      {:max_pending_requests, 0},
      {:max_connecting, 0},
      {:max_connections, 0},
      {:connections_per_endpoint, 0},
      {:max_batch_items, 0},
      {:max_batch_items, 100_001},
      {:topology_refresh_timeout, :infinity},
      {:topology_refresh_timeout, 1_000_000_000_000_000},
      {:max_refresh_candidates, 0},
      {:max_refresh_candidates, 100_001},
      {:client_name, ""},
      {:endpoint_validator, :invalid},
      {:seeds, :invalid}
    ]

    Enum.each(invalid_options, fn {key, value} ->
      opts = Keyword.put([seeds: [{"127.0.0.1", 1}], tls: true], key, value)

      assert {:error, {:invalid_client_option, ^key, ^value}} = SDK.start_link(opts)
    end)
  end

  test "a configured username requires a password before runtime startup" do
    assert {:error, {:invalid_client_option, :password, nil}} =
             SDK.start_link(seeds: [{"127.0.0.1", 1}], username: "service")
  end

  test "client names honor the current server UTF-8 and byte limits before connecting" do
    for client_name <- [<<0xFF>>, String.duplicate("x", 1_025)] do
      assert {:error, {:invalid_client_option, :client_name, ^client_name}} =
               SDK.start_link(seeds: [{"127.0.0.1", 1}], client_name: client_name)
    end
  end

  test "custom CA collections are validated before transport initialization" do
    certificate = :crypto.strong_rand_bytes(32)

    invalid_values = [
      [certificate | :invalid_tail],
      [certificate, 123],
      List.duplicate(certificate, 1_025),
      [String.duplicate("x", 1_048_577)],
      %CACerts{certificates: [certificate], fingerprint: "forged"}
    ]

    Enum.each(invalid_values, fn cacerts ->
      assert {:error, {:invalid_client_option, :cacerts, ^cacerts}} =
               SDK.start_link(seeds: [{"127.0.0.1", 1}], tls: true, cacerts: cacerts)
    end)
  end

  test "unknown startup options are rejected instead of silently using defaults" do
    assert {:error, {:invalid_client_option, :max_pending_requets, 1}} =
             SDK.start_link(
               seeds: [{"127.0.0.1", 1}],
               max_pending_requets: 1
             )
  end

  test "improper startup host lists return typed errors instead of raising" do
    improper_hosts = ["safe.example" | :invalid_tail]

    assert {:error, {:invalid_client_option, :trusted_hosts, ^improper_hosts}} =
             SDK.start_link(seeds: [{"127.0.0.1", 1}], trusted_hosts: improper_hosts)

    policy = {:allow_hosts, improper_hosts}

    assert {:error, {:invalid_client_option, :endpoint_policy, ^policy}} =
             SDK.start_link(seeds: [{"127.0.0.1", 1}], endpoint_policy: policy)

    for {key, value} <- [
          {:trusted_hosts, "safe.example"},
          {:endpoint_policy, {:allow_hosts, "safe.example"}}
        ] do
      assert {:error, {:invalid_client_option, ^key, ^value}} =
               SDK.start_link([{:seeds, [{"127.0.0.1", 1}]}, {key, value}])
    end
  end

  test "trusted host admission rejects invalid and oversized collections early" do
    seed = {"127.0.0.1", 6_388}

    for hosts <- [
          [<<0xFF>>],
          [String.duplicate("a", 256)],
          List.duplicate("safe.example", 1_025)
        ] do
      assert {:error, {:trusted_hosts, ^hosts}} =
               ClientOptions.validate(seeds: [seed], trusted_hosts: hosts)

      assert {:error, {:endpoint_policy, {:allow_hosts, ^hosts}}} =
               ClientOptions.validate(seeds: [seed], endpoint_policy: {:allow_hosts, hosts})
    end
  end

  test "malformed seed collections fail at client-option admission" do
    improper_seeds = [{"127.0.0.1", 1} | :invalid_tail]

    for seeds <- [
          improper_seeds,
          [{"", 6_388}],
          [{<<0xFF>>, 6_388}],
          [{String.duplicate("a", 256), 6_388}],
          [{"127.0.0.1", 0}],
          [%{host: "127.0.0.1"}],
          [{"127.0.0.1", 6_388}, :invalid_seed]
        ] do
      assert {:error, {:invalid_client_option, :seeds, ^seeds}} =
               SDK.start_link(seeds: seeds)
    end
  end

  test "seed admission is bounded by the configured refresh candidate budget" do
    seed = {"127.0.0.1", 6_388}
    oversized = List.duplicate(seed, 100_000)
    opts = [seeds: oversized, max_refresh_candidates: 32]

    :erlang.garbage_collect(self())
    {:reductions, before_validation} = Process.info(self(), :reductions)

    assert {:error, {:seeds, ^oversized}} = ClientOptions.validate(opts)

    {:reductions, after_validation} = Process.info(self(), :reductions)
    assert after_validation - before_validation < 20_000

    assert :ok =
             ClientOptions.validate(
               seeds: [seed, {"127.0.0.2", 6_388}],
               max_refresh_candidates: 2
             )

    assert {:error, {:seeds, [^seed, {"127.0.0.2", 6_388}, {"127.0.0.3", 6_388}]}} =
             ClientOptions.validate(
               seeds: [seed, {"127.0.0.2", 6_388}, {"127.0.0.3", 6_388}],
               max_refresh_candidates: 2
             )
  end

  test "a cached plaintext connection is never reused for a TLS endpoint" do
    {server, client} = start_sdk()
    port = NativeServer.port(server)

    assert {:error, _reason} =
             SDK.ping(client, "must-use-tls",
               endpoint: %{
                 host: "127.0.0.1",
                 native_port: port,
                 native_tls_port: 1,
                 tls: true
               },
               timeout: 100
             )

    assert NativeServer.connection_count(server) == 1
    assert Process.alive?(client)
  end

  test "a connection with different operational limits is not reused" do
    {server, client} = start_sdk()
    port = NativeServer.port(server)

    assert {:ok, "OK"} =
             SDK.ping(client, "isolated-policy",
               endpoint: %{
                 host: "127.0.0.1",
                 native_port: port,
                 tls: false,
                 heartbeat_interval: :infinity
               }
             )

    assert NativeServer.connection_count(server) == 2
    assert map_size(ClientRuntime.state(client).connection_pool.connections) == 2
  end

  test "the total cached connection count is bounded across endpoint policy profiles" do
    {server, client} = start_sdk(max_connections: 4)
    port = NativeServer.port(server)

    results =
      Enum.map(1..12, fn profile ->
        SDK.ping(client, "profile-#{profile}",
          endpoint: %{
            host: "127.0.0.1",
            native_port: port,
            max_in_flight: profile
          }
        )
      end)

    assert Enum.count(results, &(&1 == {:ok, "OK"})) == 3
    assert Enum.count(results, &(&1 == {:error, :connection_backpressure})) == 9
    assert map_size(ClientRuntime.state(client).connection_pool.connections) == 4
    assert NativeServer.connection_count(server) == 4
  end

  test "case-equivalent DNS endpoints reuse one connection" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1), host: "localhost")

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(seeds: [{"localhost", port}])

    assert {:ok, "OK"} =
             SDK.ping(client, "case-insensitive",
               endpoint: %{host: "LOCALHOST", native_port: port, tls: false}
             )

    assert NativeServer.connection_count(server) == 1
    assert map_size(ClientRuntime.state(client).connection_pool.connections) == 1

    assert :ok = SDK.close(client)
    if Process.alive?(server), do: GenServer.stop(server, :normal)
  end

  test "topology routes reuse their precomputed connection identity" do
    {_server, client} = start_sdk()
    key = "precomputed-route"
    slot = Topology.slot_for_key(key)

    :sys.replace_state(ClientRuntime.coordinator(client), fn state ->
      topology = state.topology_manager.topology
      route = elem(topology.slots, slot)
      endpoint = Map.put(route.endpoint, "host", "must-not-be-renormalized.invalid")
      route = %{route | endpoint: endpoint}
      topology = %{topology | slots: put_elem(topology.slots, slot, route)}
      manager = %{state.topology_manager | topology: topology}
      %{state | topology_manager: manager}
    end)

    assert {:ok, "OK"} = SDK.get(client, key)
    assert map_size(ClientRuntime.state(client).connection_pool.connections) == 1
  end

  test "string-keyed TLS endpoint options cannot be shadowed by plaintext defaults" do
    {server, client} = start_sdk()
    port = NativeServer.port(server)

    assert {:error, _reason} =
             SDK.ping(client, "must-use-string-keyed-tls",
               endpoint: %{
                 "host" => "127.0.0.1",
                 "native_port" => port,
                 "native_tls_port" => 1,
                 "tls" => true
               },
               timeout: 100
             )

    assert NativeServer.connection_count(server) == 1
    assert Process.alive?(client)
  end

  test "duplicate atom and string endpoint options are rejected as ambiguous" do
    {_server, client} = start_sdk()

    conflicting = %{
      "tls" => true,
      host: "127.0.0.1",
      native_port: 6_388,
      tls: false
    }

    assert {:error, {:invalid_endpoint, ^conflicting}} =
             SDK.ping(client, "ambiguous-tls", endpoint: conflicting)

    assert Process.alive?(client)
  end

  defp start_sdk(opts \\ []) do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} -> NativeServer.topology_payload(Agent.get(port_holder, & &1))
      %{opcode: 0x0001} -> %{"protocol" => "ferricstore-native"}
      _request -> "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(Keyword.put(opts, :seeds, [{"127.0.0.1", port}]))
    flush_native_server_messages()

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server, :normal)
    end)

    {server, client}
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
end
