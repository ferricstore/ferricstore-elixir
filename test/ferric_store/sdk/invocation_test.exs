defmodule FerricStore.SDK.InvocationTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Invocation

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
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    {:ok, client: client}
  end

  test "definition put accepts map input and encodes JSON" do
    assert [json] =
             Invocation.definition_put_args(%{
               name: "send-email",
               acl: %{scope_required: true}
             })

    assert %{"name" => "send-email", "acl" => %{"scope_required" => true}} =
             Jason.decode!(json)
  end

  test "definition put preserves explicit JSON input" do
    assert [~s({"name":"send-email")] =
             Invocation.definition_put_args(~s({"name":"send-email"))
  end

  test "create builds attrs envelope with optional context and idempotency key" do
    assert ["send-email", json] =
             Invocation.create_args(
               "send-email",
               %{tenant: "acme"},
               context: %{subject: "caller"},
               idempotency_key: "idem-1"
             )

    assert %{
             "attrs" => %{"tenant" => "acme"},
             "context" => %{"subject" => "caller"},
             "idempotency_key" => "idem-1"
           } = Jason.decode!(json)
  end

  test "partition list optionally scopes by namespace" do
    assert ["send-email"] = Invocation.partition_list_args("send-email")

    assert ["send-email", "SCOPE", "tenant:acme"] =
             Invocation.partition_list_args("send-email", scope: "tenant:acme")
  end

  test "helpers build Enterprise invocation commands", %{client: client} do
    assert {:ok, %{"command" => "INVOCATION.DEFINITION.PUT", "args" => [definition_json]}} =
             Invocation.put_definition(client, %{name: "send-email"})

    assert %{"name" => "send-email"} = Jason.decode!(definition_json)

    assert {:ok, %{"command" => "INVOCATION.DEFINITION.GET", "args" => ["send-email"]}} =
             Invocation.get_definition(client, "send-email")

    assert {:ok, %{"command" => "INVOCATION.DEFINITION.LIST", "args" => []}} =
             Invocation.list_definitions(client)

    assert {:ok, %{"command" => "INVOCATION.CREATE", "args" => ["send-email", create_json]}} =
             Invocation.create(client, "send-email", %{tenant: "acme"},
               request_context: %{subject: "proxy"}
             )

    assert %{"attrs" => %{"tenant" => "acme"}} = Jason.decode!(create_json)

    assert_received {:request,
                     %{
                       "command" => "INVOCATION.CREATE",
                       "request_context" => %{subject: "proxy"}
                     }, [request_context: %{subject: "proxy"}]}

    assert {:ok, %{"command" => "INVOCATION.GET", "args" => ["inv-1"]}} =
             Invocation.get(client, "inv-1")

    assert {:ok,
            %{
              "command" => "INVOCATION.PARTITION.LIST",
              "args" => ["send-email", "SCOPE", "tenant:acme"]
            }} = Invocation.list_partitions(client, "send-email", scope: "tenant:acme")
  end

  test "top-level SDK delegates expose Enterprise invocation helpers", %{client: client} do
    assert {:ok, %{"command" => "INVOCATION.DEFINITION.PUT"}} =
             SDK.invocation_definition_put(client, %{name: "send-email"})

    assert {:ok, %{"command" => "INVOCATION.DEFINITION.GET"}} =
             SDK.invocation_definition_get(client, "send-email")

    assert {:ok, %{"command" => "INVOCATION.DEFINITION.LIST"}} =
             SDK.invocation_definition_list(client)

    assert {:ok, %{"command" => "INVOCATION.CREATE"}} =
             SDK.invocation_create(client, "send-email", %{tenant: "acme"})

    assert {:ok, %{"command" => "INVOCATION.GET"}} =
             SDK.invocation_get(client, "inv-1")

    assert {:ok, %{"command" => "INVOCATION.PARTITION.LIST"}} =
             SDK.invocation_partition_list(client, "send-email")
  end
end
