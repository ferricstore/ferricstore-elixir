defmodule FerricStore.SDK.ManagementTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Management

  defmodule CaptureClient do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:request, 0x0100, payload, opts}, _from, test_pid) do
      send(test_pid, {:request, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, 0x0100, key, payload, opts}, _from, test_pid) do
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

    assert_received {:request, %{"command" => "FERRICSTORE.CAPABILITIES", "args" => []}, []}
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
                     [key: "tenant:a:*"]}
  end
end
