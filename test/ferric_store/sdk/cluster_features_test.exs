defmodule FerricStore.SDK.ClusterFeaturesTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{CommandSpec, Opcodes}
  alias FerricStore.SDK.Admin
  alias FerricStore.SDK.Native.ServerContract
  alias FerricStore.Test.NativeServer

  defmodule CaptureClient do
    use GenServer

    alias FerricStore.Test.ClientRuntime

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

    def handle_call({:request, opcode, payload, _opts}, _from, test_pid) do
      send(test_pid, {:request, opcode, payload})
      {:reply, {:ok, "supported: 1\r\nactive: 0"}, test_pid}
    end
  end

  test "cluster feature level commands use the 0.12.0 native opcodes" do
    assert Opcodes.cluster_features() == 0x0311
    assert Opcodes.cluster_features_raise() == 0x0312
    assert Opcodes.read_only?(Opcodes.cluster_features())
    refute Opcodes.read_only?(Opcodes.cluster_features_raise())
  end

  test "admin helpers send the feature level commands" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:ok, "supported: 1\r\nactive: 0"} = Admin.cluster_features(client)
    assert_received {:request, 0x0311, %{}}

    assert {:ok, _reply} = Admin.cluster_features_raise(client, %{args: ["1"]})
    assert_received {:request, 0x0312, %{"args" => ["1"]}}
  end

  test "servers from before feature levels still satisfy the connection contract" do
    opcodes =
      CommandSpec.all()
      |> Enum.reject(&(&1.name in ["CLUSTER.FEATURES", "CLUSTER.FEATURES.RAISE"]))
      |> Enum.map(&%{"name" => &1.name, "opcode" => &1.opcode})

    startup = NativeServer.startup_payload(%{"capabilities" => %{"opcodes" => opcodes}})

    assert Enum.any?(CommandSpec.all(), &(&1.name == "CLUSTER.FEATURES"))
    assert :ok = ServerContract.validate(startup)
  end
end
