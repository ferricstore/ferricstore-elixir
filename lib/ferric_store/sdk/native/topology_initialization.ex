defmodule FerricStore.SDK.Native.TopologyInitialization do
  @moduledoc false

  alias FerricStore.SDK.Native.Coordinator.State
  alias FerricStore.SDK.Native.{EventSubscriptions, TopologyBootstrap, TopologyRuntime}

  @spec run(
          State.t(),
          (State.t(), term(), pid(), map() -> State.t()),
          (State.t() -> State.t()),
          (State.t(), pid() -> State.t())
        ) :: {:ok, State.t()} | {:error, term()}
  def run(%State{} = state, track_connection, warm_connections, initialize_events)
      when is_function(track_connection, 4) and is_function(warm_connections, 1) and
             is_function(initialize_events, 2) do
    opts = [
      connection_supervisor: state.connection_supervisor,
      endpoint_policy: state.endpoint_policy,
      endpoint_trust: state.endpoint_trust,
      endpoint_validator: state.endpoint_validator,
      username: state.username,
      password: state.password,
      client_name: state.client_name,
      events:
        EventSubscriptions.management_events()
        |> EventSubscriptions.wire_payload(),
      timeout: state.topology_refresh_timeout
    ]

    case TopologyBootstrap.run(TopologyRuntime.candidates(state), opts) do
      {:ok, topology, connection, key, capacity} ->
        state =
          state
          |> TopologyRuntime.put_initial(topology)
          |> track_connection.(key, connection, capacity)
          |> TopologyRuntime.prune()
          |> warm_connections.()
          |> initialize_events.(connection)

        {:ok, state}

      {:error, _reason} = error ->
        error
    end
  end
end
