defmodule FerricStore.SDK.Native.TopologyRefreshStarter do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    EndpointPolicy,
    RefreshOperation,
    Topology,
    TopologyRefresher
  }

  @spec start([map()], ConnectionPool.t(), keyword()) ::
          {:ok, RefreshOperation.t(), ConnectionPool.t()}
          | {:error, term(), ConnectionPool.t()}
  def start(candidates, pool, opts) when is_list(candidates) and is_list(opts) do
    candidates =
      Enum.filter(candidates, fn endpoint ->
        EndpointPolicy.validate_policy(
          Keyword.fetch!(opts, :endpoint_policy),
          Keyword.fetch!(opts, :endpoint_trust),
          endpoint
        ) == :ok
      end)

    case candidates do
      [] -> {:error, :no_endpoint_reachable, pool}
      candidates -> start_refresher(candidates, pool, opts)
    end
  end

  @spec release(ConnectionPool.t(), RefreshOperation.t() | map()) :: ConnectionPool.t()
  def release(pool, %{connection_reservation: true}), do: ConnectionPool.release_refresh(pool)
  def release(pool, _operation), do: pool

  @spec cancel(RefreshOperation.t()) :: :ok
  def cancel(%RefreshOperation{refresher: refresher}) when is_pid(refresher) do
    Process.exit(refresher, :kill)
    :ok
  end

  defp start_refresher(candidates, pool, opts) do
    connections = ConnectionPool.endpoint_connections(pool)
    {strategy, reserved?, pool} = reserve_connection(pool, candidates, connections)
    token = make_ref()

    refresher_opts = [
      owner: Keyword.fetch!(opts, :owner),
      token: token,
      candidates: candidates,
      connections: connections,
      connection_supervisor: Keyword.fetch!(opts, :connection_supervisor),
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      client_name: Keyword.fetch!(opts, :client_name),
      endpoint_validator: Keyword.get(opts, :endpoint_validator),
      connection_strategy: strategy,
      timeout: Keyword.fetch!(opts, :timeout)
    ]

    case start_worker(Keyword.fetch!(opts, :operation_supervisor), refresher_opts) do
      {:ok, refresher} ->
        operation =
          RefreshOperation.new(refresher, Process.monitor(refresher), token, reserved?)

        {:ok, operation, pool}

      {:error, reason} ->
        {:error, {:topology_refresh_failed, reason},
         release(pool, %{connection_reservation: reserved?})}
    end
  end

  defp start_worker(supervisor, opts) do
    DynamicSupervisor.start_child(supervisor, {TopologyRefresher, opts})
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp reserve_connection(pool, candidates, connections) do
    replacement_available? =
      Enum.any?(candidates, fn endpoint ->
        case Map.get(connections, Topology.endpoint_key(endpoint)) do
          connection when is_pid(connection) -> true
          _missing -> false
        end
      end)

    case ConnectionPool.reserve_refresh(pool, replacement_available?) do
      {:ok, strategy, pool} -> {strategy, true, pool}
      {:error, :connection_backpressure, _pool} -> {:reuse_only, false, pool}
    end
  end
end
