defmodule FerricStore.SDK.Native.TopologyRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.ClientIdentity

  alias FerricStore.SDK.Native.{
    ClientEndpoint,
    Topology,
    TopologyCandidates,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "topology publication reports protected-table ownership failures" do
    {client, endpoint} = start_endpoint_owner()
    topology = %Topology{route_epoch: 7}

    assert {:error, :endpoint_not_owned} =
             ClientEndpoint.publish_topology(client, make_ref(), topology)

    assert :ets.lookup(endpoint, :topology) == []
  end

  test "runtime topology changes remain uncommitted when publication fails" do
    {client, endpoint} = start_endpoint_owner()
    state = %State{runtime_supervisor: client}
    topology = %Topology{route_epoch: 8}

    assert {:error, {:topology_publication_failed, :endpoint_not_owned}} =
             TopologyRuntime.put(state, topology)

    assert TopologyRuntime.current(state) == nil
    assert :ets.lookup(endpoint, :topology) == []
  end

  test "initial topology installation is explicitly unpublished before endpoint handoff" do
    {client, endpoint} = start_endpoint_owner()
    state = %State{runtime_supervisor: client}
    topology = %Topology{route_epoch: 9}

    initialized = TopologyRuntime.put_initial(state, topology)

    assert TopologyRuntime.current(initialized) == topology
    assert :ets.lookup(endpoint, :topology) == []
  end

  test "refresh candidates reserve capacity for current topology endpoints" do
    live_endpoint = %{host: "live.internal", native_port: 7_000}
    live_key = Topology.endpoint_key(live_endpoint)

    seeds =
      Enum.map(1..40, fn index ->
        %{host: "seed-#{index}.internal", native_port: 6_000 + index}
      end)

    state =
      %State{seeds: seeds, tls: false, max_refresh_candidates: 32}
      |> TopologyRuntime.put_initial(%Topology{endpoints: %{live_key => live_endpoint}})

    candidates = TopologyRuntime.candidates(state)

    assert length(candidates) == 32
    assert Enum.at(candidates, 1).host == "live.internal"
    assert Enum.any?(candidates, &(Topology.endpoint_key(&1) == live_key))
  end

  test "bounded refresh candidates retain configured fallback seeds" do
    [primary, fallback] = [
      %{host: "primary.internal", native_port: 6_001},
      %{host: "fallback.internal", native_port: 6_002}
    ]

    endpoints =
      Map.new(1..40, fn index ->
        endpoint = %{host: "node-#{index}.internal", native_port: 7_000 + index}
        {Topology.endpoint_key(endpoint), endpoint}
      end)

    state =
      %State{seeds: [primary, fallback], tls: false, max_refresh_candidates: 32}
      |> TopologyRuntime.put_initial(%Topology{endpoints: endpoints})

    candidate_keys = state |> TopologyRuntime.candidates() |> Enum.map(&Topology.endpoint_key/1)

    assert length(candidate_keys) == 32
    assert Topology.endpoint_key(primary) in candidate_keys
    assert Topology.endpoint_key(fallback) in candidate_keys
  end

  test "discovered refresh and control endpoints use deterministic transport order" do
    endpoints =
      Map.new(["zeta.internal", "alpha.internal", "middle.internal"], fn host ->
        endpoint = %{host: host, native_port: 7_000}
        {Topology.endpoint_key(endpoint), endpoint}
      end)

    state =
      %State{seeds: [], tls: false, max_refresh_candidates: 32}
      |> TopologyRuntime.put_initial(%Topology{endpoints: endpoints})

    expected_keys =
      endpoints |> Map.values() |> Enum.map(&Topology.endpoint_key/1) |> Enum.sort()

    assert state |> TopologyRuntime.candidates() |> Enum.map(&Topology.endpoint_key/1) ==
             expected_keys

    assert state |> TopologyRuntime.control_endpoint() |> Topology.endpoint_key() ==
             hd(expected_keys)
  end

  test "refresh candidate selection computes each transport identity only once" do
    seeds =
      Enum.map(1..32, fn index ->
        %{host: "seed-#{index}.internal", native_port: 6_000 + index}
      end)

    discovered =
      Enum.map(1..1_024, fn index ->
        %{host: "node-#{rem(index * 7_919, 1_024)}.internal", native_port: 7_000 + index}
      end)

    :erlang.garbage_collect(self())
    {:reductions, before_selection} = Process.info(self(), :reductions)

    Enum.each(1..10, fn _iteration ->
      assert length(TopologyCandidates.select(seeds, discovered, 32)) == 32
    end)

    {:reductions, after_selection} = Process.info(self(), :reductions)
    assert after_selection - before_selection < 5_000_000
  end

  test "control endpoint lookup is constant-time after topology construction" do
    ranges =
      Enum.map(0..1_023, fn slot ->
        %{
          "first_slot" => slot,
          "last_slot" => slot,
          "shard" => slot,
          "lane_id" => slot + 1,
          "host" => "node-#{slot}.internal",
          "native_port" => 7_000 + slot
        }
      end)

    assert {:ok, topology} =
             Topology.build(%{
               "route_epoch" => 1,
               "shard_count" => 1_024,
               "ranges" => ranges
             })

    state = TopologyRuntime.put_initial(%State{seeds: [], tls: false}, topology)
    :erlang.garbage_collect(self())
    {:reductions, before_lookup} = Process.info(self(), :reductions)

    Enum.each(1..100, fn _iteration ->
      assert %{host: "node-0.internal"} = TopologyRuntime.control_endpoint(state)
    end)

    {:reductions, after_lookup} = Process.info(self(), :reductions)
    assert after_lookup - before_lookup < 50_000
  end

  defp start_endpoint_owner do
    parent = self()

    client =
      spawn_link(fn ->
        endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
        true = :ets.insert(endpoint, {:client, self()})
        ClientIdentity.mark(:topology_aware, endpoint)
        send(parent, {:endpoint_owner, self(), endpoint})

        receive do
          :stop -> :ok
        end
      end)

    assert_receive {:endpoint_owner, ^client, endpoint}, 1_000
    on_exit(fn -> if Process.alive?(client), do: send(client, :stop) end)
    {client, endpoint}
  end
end
