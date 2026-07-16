defmodule FerricStore.SDK.Native.TopologyRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ClientEndpoint,
    ConnectionLifecycle,
    EndpointPolicy,
    Topology,
    TopologyCandidates,
    TopologyManager
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec current(State.t()) :: Topology.t() | nil
  def current(%State{} = state), do: TopologyManager.topology(state.topology_manager)

  @spec put(State.t(), Topology.t()) ::
          {:ok, State.t()} | {:error, {:topology_publication_failed, term()}}
  def put(%State{} = state, %Topology{} = topology) do
    manager = TopologyManager.put_topology(state.topology_manager, topology)
    {version, topology} = TopologyManager.snapshot(manager)

    case ClientEndpoint.publish_topology(state.runtime_supervisor, version, topology) do
      :ok -> {:ok, %{state | topology_manager: manager}}
      {:error, reason} -> {:error, {:topology_publication_failed, reason}}
    end
  end

  @spec put_initial(State.t(), Topology.t()) :: State.t()
  def put_initial(%State{} = state, %Topology{} = topology) do
    manager = TopologyManager.put_topology(state.topology_manager, topology)
    %{state | topology_manager: manager}
  end

  @spec control_endpoint(State.t()) :: map()
  def control_endpoint(%State{} = state) do
    case current(state) do
      %Topology{control_endpoint: endpoint} when is_map(endpoint) ->
        endpoint

      %Topology{endpoints: endpoints} ->
        TopologyCandidates.control(
          Map.values(endpoints),
          state.seeds,
          %{host: "127.0.0.1", native_port: 6379}
        )

      _missing_topology ->
        List.first(state.seeds) || %{host: "127.0.0.1", native_port: 6379}
    end
  end

  @spec candidates(State.t()) :: [map()]
  def candidates(%State{} = state) do
    topology_endpoints =
      case current(state) do
        %Topology{endpoints: endpoints} -> Map.values(endpoints)
        _other -> []
      end

    seeds = Enum.map(state.seeds, &endpoint_defaults(&1, state))
    topology_endpoints = Enum.map(topology_endpoints, &endpoint_defaults(&1, state))

    TopologyCandidates.select(seeds, topology_endpoints, state.max_refresh_candidates)
  end

  @spec prune(State.t()) :: State.t()
  def prune(%State{} = state) do
    topology_keys = state |> current() |> Map.fetch!(:endpoints) |> Map.keys() |> MapSet.new()
    seed_keys = state.seeds |> Enum.map(&Topology.endpoint_key/1) |> MapSet.new()
    keep_keys = MapSet.union(topology_keys, seed_keys)

    pool = ConnectionLifecycle.prune(state.connection_pool, keep_keys)
    %{state | connection_pool: pool}
  end

  @spec endpoint_defaults(map(), State.t()) :: map()
  def endpoint_defaults(endpoint, %State{} = state) do
    endpoint
    |> Map.put_new(:tls, state.tls)
    |> EndpointPolicy.apply_options(state.endpoint_options || %{})
    |> EndpointPolicy.put_server_name(state.server_name)
    |> Map.put(:event_handler, self())
  end
end
