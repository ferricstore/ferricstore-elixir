defmodule FerricStore.SDK.AdminTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Admin
  alias FerricStore.SDK.Native.Opcodes

  defmodule CaptureClient do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:request, opcode, payload, opts}, _from, test_pid) do
      send(test_pid, {:request, opcode, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, opcode, key, payload, opts}, _from, test_pid) do
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
end
