defmodule FerricStore.SDK.InvocationTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Invocation
  alias FerricStore.Test.{ClientRuntime, ExplodingJSON, SlowJSON}

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
    json = ~s({"name":"send-email"})
    assert [^json] = Invocation.definition_put_args(json)
  end

  test "definition put rejects malformed and non-object JSON locally" do
    assert {:error,
            {:invalid_invocation_input,
             %{
               operation: :put_definition,
               field: :definition,
               reason: :invalid_json,
               value: :redacted
             }}} = Invocation.definition_put_args(~s({"secret":"value"))

    assert {:error,
            {:invalid_invocation_input,
             %{
               operation: :put_definition,
               field: :definition,
               reason: :expected_json_object,
               value: :redacted
             }}} = Invocation.definition_put_args(Jason.encode!([]))
  end

  test "definition put rejects ambiguous duplicate JSON object keys" do
    for definition <- [
          ~s({"name":"safe","name":"shadowed"}),
          ~s({"name":"safe","acl":{"scope":true,"scope":false}})
        ] do
      assert {:error,
              {:invalid_invocation_input,
               %{
                 operation: :put_definition,
                 field: :definition,
                 reason: :duplicate_json_object_key,
                 value: :redacted
               }}} = Invocation.definition_put_args(definition)
    end
  end

  test "JSON map input rejects keys that encode to the same object field" do
    assert {:error,
            {:invalid_invocation_input,
             %{
               operation: :put_definition,
               field: :definition,
               reason: :not_json_encodable
             }}} = Invocation.definition_put_args(%{:name => "safe", "name" => "shadowed"})

    assert {:error,
            {:invalid_invocation_input,
             %{operation: :create, field: :payload, reason: :not_json_encodable}}} =
             Invocation.create_args("send-email", %{:tenant => "a", "tenant" => "b"})
  end

  test "JSON encoder callback failures remain typed local input errors" do
    assert {:error,
            {:invalid_invocation_input,
             %{operation: :put_definition, field: :definition, reason: :not_json_encodable}}} =
             Invocation.definition_put_args(%{value: %ExplodingJSON{}})
  end

  test "JSON input rejects custom encoders without invoking user callbacks" do
    value = %SlowJSON{owner: self(), delay: 25}

    assert {:error,
            {:invalid_invocation_input,
             %{
               operation: :put_definition,
               field: :definition,
               reason: :not_json_encodable,
               value: %{value: ^value}
             }}} = Invocation.definition_put_args(%{value: value})

    refute_received {:slow_json_encoder, _encoder}
  end

  test "request deadlines are established before invocation JSON encoding", %{client: client} do
    attrs = %{"items" => Enum.to_list(1..100_000)}
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Invocation.create(client, "send-email", attrs, timeout: 0, call_timeout: 0)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
    refute_received {:request, _payload, _opts}
  end

  test "invocation requests reject custom JSON encoders before callbacks", %{client: client} do
    attrs = %{"value" => %SlowJSON{owner: self()}}

    assert {:error,
            {:invalid_invocation_input,
             %{operation: :create, field: :payload, reason: :not_json_encodable}}} =
             Invocation.create(client, "send-email", attrs, timeout: 1_000)

    refute_received {:slow_json_encoder, _encoder}
    refute_received {:request, _payload, _opts}
  end

  test "invocation JSON validation stops when its request deadline expires", %{client: client} do
    definition = Jason.encode!(%{"name" => "large", "items" => Enum.to_list(1..500_000)})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Invocation.put_definition(client, definition, timeout: 1)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 250_000
    refute_received {:request, _payload, _opts}
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

    assert_received {:request, %{"command" => "INVOCATION.DEFINITION.GET"}, [idempotent: true]}

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
                       "request_context" => %{"subject" => "proxy"}
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

  test "invocation helpers return typed errors for malformed public inputs", %{client: client} do
    cases = [
      {:put_definition, :definition, :expected_map_or_binary,
       fn -> Invocation.put_definition(client, self()) end},
      {:get_definition, :name, :expected_nonempty_binary,
       fn -> Invocation.get_definition(client, "") end},
      {:get, :id, :expected_nonempty_binary, fn -> Invocation.get(client, :invalid) end},
      {:create, :attrs, :expected_map,
       fn -> Invocation.create(client, "send-email", :invalid) end},
      {:list_partitions, :scope, :expected_binary,
       fn -> Invocation.list_partitions(client, "send-email", scope: 123) end}
    ]

    for {operation, field, reason, call} <- cases do
      assert {:error,
              {:invalid_invocation_input,
               %{operation: ^operation, field: ^field, reason: ^reason}}} = call.()
    end

    malformed_options = [{:scope, "tenant:acme"} | :invalid_tail]

    assert {:error, {:invalid_request_option, :options, ^malformed_options}} =
             Invocation.list_definitions(client, malformed_options)

    refute_received {:request, _payload, _opts}
  end
end
