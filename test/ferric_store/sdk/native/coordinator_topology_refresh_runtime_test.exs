defmodule FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.ClientIdentity

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    CoordinatorTopologyRefreshRuntime,
    LifecycleRegistry,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "publication failure rolls back a newly refreshed connection" do
    supervisor = start_supervised!({DynamicSupervisor, strategy: :one_for_one})
    runtime_supervisor = start_endpoint_owner()
    connection = start_connection(supervisor)
    monitor = Process.monitor(connection)
    key = {:endpoint, 1}
    topology = %Topology{route_epoch: 9}

    state = %State{
      runtime_supervisor: runtime_supervisor,
      connection_supervisor: supervisor
    }

    assert {:noreply, next_state} =
             CoordinatorTopologyRefreshRuntime.finish(
               state,
               [],
               {:ok, topology, connection, key, %{}, nil},
               %{}
             )

    refute ConnectionPool.connection?(next_state.connection_pool, connection)
    assert LifecycleRegistry.empty?(next_state.lifecycle_registry)
    assert TopologyRuntime.current(next_state) == nil
    assert_receive {:DOWN, ^monitor, :process, ^connection, :shutdown}, 250
  end

  defp start_connection(supervisor) do
    {:ok, connection} =
      DynamicSupervisor.start_child(supervisor, {Agent, fn -> :connection end})

    connection
  end

  defp start_endpoint_owner do
    parent = self()

    owner =
      spawn_link(fn ->
        endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
        true = :ets.insert(endpoint, {:client, self()})
        ClientIdentity.mark(:topology_aware, endpoint)
        send(parent, {:endpoint_owner, self()})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:endpoint_owner, ^owner}, 1_000
    on_exit(fn -> if Process.alive?(owner), do: send(owner, :stop) end)
    owner
  end
end
