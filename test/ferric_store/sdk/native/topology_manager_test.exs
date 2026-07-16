defmodule FerricStore.SDK.Native.TopologyManagerTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{Topology, TopologyManager}

  test "identical topology refreshes preserve the routing snapshot version" do
    topology = %Topology{route_epoch: 7, shard_count: 1}
    manager = TopologyManager.put_topology(%TopologyManager{}, topology)
    version = TopologyManager.version(manager)

    refreshed = TopologyManager.put_topology(manager, topology)

    assert TopologyManager.version(refreshed) == version
    assert TopologyManager.topology(refreshed) == topology
  end

  test "changed topology views install even when the route epoch is unchanged" do
    current = %Topology{route_epoch: 7, shard_count: 1}
    candidate = %Topology{route_epoch: 7, shard_count: 2}
    manager = TopologyManager.put_topology(%TopologyManager{}, current)
    version = TopologyManager.version(manager)

    refreshed = TopologyManager.put_topology(manager, candidate)

    refute TopologyManager.version(refreshed) == version
    assert TopologyManager.topology(refreshed) == candidate
  end
end
