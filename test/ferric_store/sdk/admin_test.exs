defmodule FerricStore.SDK.AdminTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Admin
  alias FerricStore.Test.ClientRuntime

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

    def handle_call({:request, opcode, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request, opcode, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, opcode, key, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request_by_key, opcode, key, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    {:ok, client: client}
  end

  test "cluster admin helpers use typed native admin opcodes", %{client: client} do
    assert {:ok, %{}} = Admin.cluster_health(client)

    assert_received {:request, opcode, %{}, []}
    assert opcode == Opcodes.cluster_health()
  end

  test "key-scoped admin helpers route by payload key", %{client: client} do
    assert {:ok, %{"key" => "tenant:a:k"}} =
             Admin.ferricstore_key_info(client, %{key: "tenant:a:k"})

    assert_received {:request_by_key, opcode, "tenant:a:k", %{"key" => "tenant:a:k"}, []}
    assert opcode == Opcodes.ferricstore_key_info()
  end

  test "admin payload normalization rejects colliding atom and string keys", %{client: client} do
    assert {:error, {:duplicate_normalized_map_key, "key"}} =
             Admin.ferricstore_key_info(client, %{:key => "atom", "key" => "string"})

    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "admin helpers reject non-map payload containers without raising", %{client: client} do
    for call <- [
          fn -> Admin.cluster_health(client, :not_a_map) end,
          fn -> Admin.request(client, :cluster_health, :not_a_map) end
        ] do
      assert {:error, {:invalid_admin_payload, %{reason: :expected_map, value: :not_a_map}}} =
               call.()
    end

    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "admin normalization returns unsupported nested map keys as typed errors", %{
    client: client
  } do
    invalid_key = {:unsupported, :map_key}

    assert {:error, {:invalid_map_key, ^invalid_key}} =
             Admin.cluster_health(client, %{"nested" => %{invalid_key => "value"}})

    refute_received {:request, _opcode, _payload, _opts}
  end

  test "admin deadlines are established before recursive payload normalization", %{client: client} do
    payload = %{"items" => Enum.to_list(1..100_000)}
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Admin.cluster_health(client, payload, timeout: 0, call_timeout: 0)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
    refute_received {:request, _opcode, _payload, _opts}
  end

  test "invalid explicit admin route keys fail without sending a request", %{client: client} do
    assert {:error, {:invalid_route_key, 123}} =
             Admin.ferricstore_key_info(client, %{key: "valid-payload-key"}, route_key: 123)

    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "valid explicit admin route keys are consumed before transport", %{client: client} do
    assert {:ok, %{}} =
             Admin.cluster_health(client, %{}, route_key: "tenant:a", timeout: 321)

    assert_received {:request_by_key, opcode, "tenant:a", %{}, [timeout: 321]}
    assert opcode == Opcodes.cluster_health()
  end
end
