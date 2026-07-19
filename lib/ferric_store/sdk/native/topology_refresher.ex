defmodule FerricStore.SDK.Native.TopologyRefresher do
  @moduledoc false

  use GenServer

  alias FerricStore.DeadlineBudget

  alias FerricStore.SDK.Native.{
    ConnectionLifecycle,
    EndpointValidator,
    Topology,
    TopologyRefreshCandidates,
    TopologyRefreshConnection,
    TopologyReplacementDrain
  }

  @default_timeout 5_000

  @derive {Inspect, except: [:password]}
  defstruct [
    :owner,
    :token,
    :candidates,
    :connections,
    :connection_supervisor,
    :username,
    :password,
    :client_name,
    :endpoint_validator,
    :connection_strategy,
    :deadline,
    timeout: @default_timeout
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    state = struct!(__MODULE__, opts)
    {:ok, %{state | deadline: DeadlineBudget.new(state.timeout)}, {:continue, :refresh}}
  end

  @impl true
  def handle_continue(:refresh, state) do
    result = TopologyRefreshCandidates.run(state.candidates, state, &refresh_candidate/2)

    send(state.owner, {:ferricstore_topology_refreshed, self(), state.token, result})
    {:stop, :normal, state}
  end

  defp refresh_candidate(endpoint, state) do
    with {:ok, timeout} <- request_timeout(state),
         :ok <- EndpointValidator.validate(state.endpoint_validator, endpoint, timeout) do
      do_refresh_candidate(endpoint, state)
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp do_refresh_candidate(endpoint, state) do
    key = Topology.endpoint_key(endpoint)

    case Map.get(state.connections, key) do
      conn when is_pid(conn) ->
        if Process.alive?(conn) do
          load_existing_topology(conn, endpoint, key, state)
        else
          replace_or_connect(conn, endpoint, key, state, :closed)
        end

      _other ->
        connect_without_existing(endpoint, key, state)
    end
  end

  defp load_existing_topology(conn, endpoint, key, state) do
    case TopologyRefreshConnection.load(conn, endpoint, key, state) do
      {:ok, topology, conn, key, capacity} ->
        {:ok, topology, conn, key, capacity, nil}

      {:error, reason} ->
        replace_or_connect(conn, endpoint, key, state, reason)
    end
  catch
    :exit, reason -> replace_or_connect(conn, endpoint, key, state, reason)
  end

  defp connect_without_existing(endpoint, key, %{connection_strategy: :new} = state),
    do: connect_and_load(endpoint, key, state, nil)

  defp connect_without_existing(_endpoint, _key, _state),
    do: {:error, :connection_backpressure}

  defp replace_or_connect(conn, endpoint, key, %{connection_strategy: :new} = state, _reason),
    do: connect_and_load(endpoint, key, state, conn)

  defp replace_or_connect(
         conn,
         endpoint,
         key,
         %{connection_strategy: :replacement} = state,
         _reason
       ) do
    case TopologyReplacementDrain.await(conn, state) do
      :ok -> connect_and_load(endpoint, key, state, conn)
      {:error, _reason} = error -> error
    end
  end

  defp replace_or_connect(_conn, _endpoint, _key, _state, reason), do: {:error, reason}

  defp connect_and_load(endpoint, key, state, replaced_connection) do
    endpoint = TopologyRefreshConnection.with_connect_timeout(endpoint, state)

    case ConnectionLifecycle.start(state.connection_supervisor, endpoint) do
      {:ok, conn} ->
        load_started_connection(conn, endpoint, key, state, replaced_connection)

      {:error, _reason} = error ->
        error
    end
  end

  defp load_started_connection(conn, endpoint, key, state, replaced_connection) do
    Process.link(conn)
    result = TopologyRefreshConnection.bootstrap(conn, endpoint, key, state)

    case result do
      {:ok, topology, conn, key, capacity} ->
        {:ok, topology, conn, key, capacity, replaced_connection}

      {:error, _reason} = error ->
        ConnectionLifecycle.stop(state.connection_supervisor, conn)
        error
    end
  catch
    :exit, reason ->
      ConnectionLifecycle.stop(state.connection_supervisor, conn)
      {:error, reason}
  end

  defp request_timeout(state), do: DeadlineBudget.request_timeout(state.deadline)
end
