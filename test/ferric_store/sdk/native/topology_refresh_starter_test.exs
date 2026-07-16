defmodule FerricStore.SDK.Native.TopologyRefreshStarterTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{ConnectionPool, EndpointTrust, Topology, TopologyRefreshStarter}

  test "a stopped operation supervisor returns an error and releases refresh capacity" do
    supervisor = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(supervisor)
    send(supervisor, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^supervisor, :normal}

    pool = ConnectionPool.new(max_connections: 1, max_connecting: 1)
    endpoint = Topology.prepare_endpoint(%{host: "127.0.0.1", native_port: 6_388})

    assert {:error, {:topology_refresh_failed, {:exit, _reason}}, returned_pool} =
             TopologyRefreshStarter.start([endpoint], pool,
               owner: self(),
               operation_supervisor: supervisor,
               connection_supervisor: self(),
               endpoint_policy: :any,
               endpoint_trust: %EndpointTrust{},
               client_name: "refresh-starter-test",
               timeout: 100
             )

    assert returned_pool.refresh_reservations == 0
    refute ConnectionPool.full?(returned_pool)
  end
end
