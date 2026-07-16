defmodule FerricStore.SDK.ManagementTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Management
  alias FerricStore.Test.{ClientRuntime, ExplodingString, ObservableString}

  defmodule CaptureClient do
    use GenServer

    def start_link(test_pid),
      do:
        GenServer.start_link(__MODULE__, test_pid)
        |> ClientRuntime.wrap()

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({:request, 0x0100, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, 0x0100, key, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request_by_key, key, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    {:ok, client: client}
  end

  test "capabilities builds the stable management probe", %{client: client} do
    assert {:ok, %{"command" => "FERRICSTORE.CAPABILITIES", "args" => []}} =
             Management.capabilities(client)

    assert_received {:request, %{"command" => "FERRICSTORE.CAPABILITIES", "args" => []},
                     [idempotent: true]}
  end

  test "management reads reject malformed options without raising", %{client: client} do
    improper = [{:timeout, 10} | :invalid_tail]

    for opts <- [:not_a_keyword, improper],
        call <- [
          fn -> Management.capabilities(client, opts) end,
          fn -> Management.get_user(client, "worker", opts) end
        ] do
      assert {:error, {:invalid_request_option, :options, ^opts}} = call.()
    end

    refute_received {:request, _payload, _opts}
  end

  test "ACL helpers build narrow ACL commands", %{client: client} do
    assert {:ok, %{"command" => "ACL", "args" => ["SETUSER", "platform_worker_abcd" | rules]}} =
             Management.set_user(client, "platform_worker_abcd", [
               :on,
               ">secret",
               "+PING",
               "+@read",
               "-@dangerous",
               "~tenant:namespace:*"
             ])

    assert rules == [
             "on",
             ">secret",
             "+PING",
             "+@read",
             "-@dangerous",
             "~tenant:namespace:*"
           ]

    assert {:ok, %{"args" => ["DELUSER", "platform_worker_abcd"]}} =
             Management.del_user(client, "platform_worker_abcd")

    assert {:ok, %{"args" => ["GETUSER", "platform_worker_abcd"]}} =
             Management.get_user(client, "platform_worker_abcd")

    assert {:ok, %{"args" => ["LIST"]}} = Management.list_users(client)
    assert {:ok, %{"args" => ["SAVE"]}} = Management.save_acl(client)
  end

  test "ACL helpers reject malformed usernames and rule collections locally", %{client: client} do
    for username <- [:worker, ""] do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: :set_user,
                 field: :username,
                 reason: :expected_nonempty_binary,
                 value: ^username
               }}} = Management.set_user(client, username, ["on"])
    end

    invalid_rule = %{allow: "PING"}

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_user,
               field: :rules,
               reason: :invalid_rule,
               index: 1,
               value: ^invalid_rule
             }}} = Management.set_user(client, "worker", ["on", invalid_rule])

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_user,
               field: :rules,
               reason: :improper_list,
               index: 1
             }}} = Management.set_user(client, "worker", ["on" | :invalid_tail])

    refute_received {:request, _payload, _opts}
  end

  test "ACL rule conversion callbacks cannot throw through the public boundary", %{client: client} do
    assert {:error,
            {:invalid_management_input,
             %{operation: :set_user, field: :rules, reason: :invalid_rule, index: 0}}} =
             Management.set_user(client, "worker", [%ExplodingString{}])

    refute_received {:request, _payload, _opts}
  end

  test "ACL rule normalization never executes user-defined string callbacks", %{client: client} do
    rule = %ObservableString{owner: self(), value: "+PING"}

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_user,
               field: :rules,
               reason: :invalid_rule,
               index: 0,
               value: ^rule
             }}} = Management.set_user(client, "worker", [rule])

    refute_received :string_chars_called
    refute_received {:request, _payload, _opts}
  end

  test "management deadlines are established before collection preprocessing", %{client: client} do
    rules = List.duplicate("on", 50_000)
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Management.set_user(client, "worker", rules, timeout: 0, call_timeout: 0)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 50_000
    refute_received {:request, _payload, _opts}
  end

  test "management collection preprocessing stops when its deadline expires", %{client: client} do
    quota = Map.new(1..49_000, &{"quota-#{&1}", &1})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Management.set_quota(client, "tenant:a", quota, timeout: 5)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000_000
    refute_received {:request, _payload, _opts}
  end

  test "ACL rule admission stops before constructing an impossible command", %{client: client} do
    rules = List.duplicate("on", 99_999)

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_user,
               field: :rules,
               reason: :too_many_items,
               limit: 99_998,
               observed: 99_999
             }}} = Management.set_user(client, "worker", rules)

    refute_received {:request, _payload, _opts}
  end

  test "management identifiers reject malformed and empty values locally", %{client: client} do
    calls = [
      {:del_user, :username, fn value -> Management.del_user(client, value) end},
      {:get_user, :username, fn value -> Management.get_user(client, value) end},
      {:ensure_namespace, :prefix, fn value -> Management.ensure_namespace(client, value) end},
      {:get_namespace, :prefix, fn value -> Management.get_namespace(client, value) end},
      {:delete_namespace, :prefix, fn value -> Management.delete_namespace(client, value) end},
      {:set_quota, :namespace, fn value -> Management.set_quota(client, value, %{keys: 1}) end},
      {:get_quota, :namespace, fn value -> Management.get_quota(client, value) end},
      {:quota_usage, :namespace, fn value -> Management.quota_usage(client, value) end},
      {:namespace_usage, :prefix, fn value -> Management.namespace_usage(client, value) end},
      {:flow_history, :id, fn value -> Management.flow_history(client, value) end}
    ]

    for {operation, field, call} <- calls, value <- [:invalid, ""] do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: ^operation,
                 field: ^field,
                 reason: :expected_nonempty_binary,
                 value: ^value
               }}} = call.(value)
    end

    refute_received {:request, _payload, _opts}
  end

  test "namespace helpers build scoped namespace commands", %{client: client} do
    assert {:ok,
            %{
              "command" => "FERRICSTORE.NAMESPACE",
              "args" => ["ENSURE", "tenant:a", "DURABILITY", "raft", "FLOW_COUNT", 10]
            }} =
             Management.ensure_namespace(client, "tenant:a",
               durability: :raft,
               flow_count: 10,
               ignored: nil
             )

    assert {:ok, %{"args" => ["GET", "tenant:a"]}} =
             Management.get_namespace(client, "tenant:a")

    assert {:ok, %{"args" => ["LIST"]}} = Management.list_namespaces(client)

    assert {:ok, %{"args" => ["DELETE", "tenant:a"]}} =
             Management.delete_namespace(client, "tenant:a")
  end

  test "quota helpers build quota commands", %{client: client} do
    assert {:ok,
            %{
              "command" => "FERRICSTORE.QUOTA",
              "args" => ["SET", "tenant:a", "KEYS", 10, "OPS_PER_SEC", 100]
            }} =
             Management.set_quota(client, "tenant:a", keys: 10, ops_per_sec: 100)

    assert {:ok, %{"args" => ["GET", "tenant:a"]}} = Management.get_quota(client, "tenant:a")
    assert {:ok, %{"args" => ["USAGE", "tenant:a"]}} = Management.quota_usage(client, "tenant:a")
  end

  test "safe telemetry helpers build telemetry commands", %{client: client} do
    assert {:ok, %{"command" => "FERRICSTORE.TELEMETRY", "args" => ["CLUSTER_INFO"]}} =
             Management.cluster_info(client)

    assert {:ok, %{"args" => ["NAMESPACE_USAGE", "tenant:a"]}} =
             Management.namespace_usage(client, "tenant:a")

    assert {:ok, %{"args" => ["FLOW_QUERY", "PARTITION", "tenant:a", "STATE", "running"]}} =
             Management.flow_query(client, partition: "tenant:a", state: "running")

    assert {:ok, %{"args" => ["FLOW_HISTORY", "flow-1", "PARTITION", "tenant:a"]}} =
             Management.flow_history(client, "flow-1", partition: "tenant:a")
  end

  test "management attribute pairs reject malformed and ambiguous inputs locally", %{
    client: client
  } do
    calls = [
      {:ensure_namespace, :attrs,
       fn ->
         Management.ensure_namespace(client, "tenant:a", :invalid)
       end},
      {:set_quota, :quota_spec, fn -> Management.set_quota(client, "tenant:a", :invalid) end},
      {:flow_query, :attrs, fn -> Management.flow_query(client, :invalid) end},
      {:flow_history, :attrs, fn -> Management.flow_history(client, "flow-1", :invalid) end}
    ]

    Enum.each(calls, fn {operation, field, call} ->
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: ^operation,
                 field: ^field,
                 reason: :expected_map_or_pair_list,
                 value: :invalid
               }}} = call.()
    end)

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_quota,
               field: :quota_spec,
               reason: :improper_list,
               index: 1
             }}} = Management.set_quota(client, "tenant:a", [{:keys, 1} | :invalid_tail])

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_quota,
               field: :quota_spec,
               reason: :expected_pair,
               index: 0,
               value: :invalid_pair
             }}} = Management.set_quota(client, "tenant:a", [:invalid_pair])

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_quota,
               field: :quota_spec,
               reason: :duplicate_keys,
               keys: ["KEYS"]
             }}} = Management.set_quota(client, "tenant:a", [{:keys, 1}, {"keys", 2}])

    for blank_key <- ["", :""] do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: :set_quota,
                 field: :quota_spec,
                 reason: :invalid_key,
                 index: 0,
                 value: ^blank_key
               }}} = Management.set_quota(client, "tenant:a", [{blank_key, 1}])
    end

    invalid_utf8_key = <<255>>

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_quota,
               field: :quota_spec,
               reason: :invalid_key,
               index: 0,
               value: ^invalid_utf8_key
             }}} = Management.set_quota(client, "tenant:a", [{invalid_utf8_key, 1}])

    refute_received {:request, _payload, _opts}
  end

  test "management attribute keys are bounded before Unicode normalization", %{client: client} do
    oversized_key = String.duplicate("field", 200_000)
    {:reductions, before_validation} = Process.info(self(), :reductions)

    assert {:error,
            {:invalid_management_input,
             %{
               operation: :set_quota,
               field: :quota_spec,
               reason: :invalid_key,
               index: 0,
               value: ^oversized_key
             }}} = Management.set_quota(client, "tenant:a", [{oversized_key, 1}])

    {:reductions, after_validation} = Process.info(self(), :reductions)
    assert after_validation - before_validation < 10_000
    refute_received {:request, _payload, _opts}
  end

  test "management pair values reject non-scalar and out-of-wire-domain terms locally", %{
    client: client
  } do
    invalid_values = [[], %{}, {:tuple, "value"}, self(), 9_223_372_036_854_775_808]

    for value <- invalid_values do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: :ensure_namespace,
                 field: :attrs,
                 reason: :invalid_value,
                 index: 0
               }}} = Management.ensure_namespace(client, "tenant:a", [{:field, value}])
    end

    refute_received {:request, _payload, _opts}
  end

  test "management pair admission rejects lists and maps before doubling them into args", %{
    client: client
  } do
    pairs = Enum.map(1..50_000, &{"field_#{&1}", &1})
    fields = Map.new(pairs)

    for attrs <- [pairs, fields] do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: :ensure_namespace,
                 field: :attrs,
                 reason: :too_many_items,
                 limit: 49_999,
                 observed: 50_000
               }}} = Management.ensure_namespace(client, "tenant:a", attrs)
    end

    refute_received {:request, _payload, _opts}
  end

  test "top-level SDK delegates expose management helpers", %{client: client} do
    assert {:ok, %{"command" => "FERRICSTORE.CAPABILITIES"}} = SDK.capabilities(client)

    assert {:ok, %{"args" => ["SETUSER", "u", "on"]}} =
             SDK.acl_set_user(client, "u", "on")

    assert {:ok, %{"args" => ["SET", "tenant:a", "KEYS", 1]}} =
             SDK.set_quota(client, "tenant:a", keys: 1)
  end

  test "management commands can route through a scoped key when requested", %{client: client} do
    assert {:ok, %{"command" => "FERRICSTORE.QUOTA"}} =
             Management.get_quota(client, "tenant:a", key: "tenant:a:*")

    assert_received {:request_by_key, "tenant:a:*",
                     %{"command" => "FERRICSTORE.QUOTA", "args" => ["GET", "tenant:a"]},
                     [idempotent: true, key: "tenant:a:*"]}
  end
end
