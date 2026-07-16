defmodule FerricStore.SDK.Native.TopologyRefreshConnection do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.Native.{Connection, SessionBootstrap}

  @spec with_connect_timeout(map(), map()) :: map()
  def with_connect_timeout(endpoint, state) do
    put_connect_timeout(endpoint, remaining_timeout(state))
  end

  @spec bootstrap(pid(), map(), term(), map()) ::
          {:ok, term(), pid(), term(), map()} | {:error, term()}
  def bootstrap(conn, endpoint, key, state) do
    with {:ok, topology} <-
           SessionBootstrap.establish(conn,
             client_name: state.client_name,
             username: state.username,
             password: state.password,
             topology_endpoint: endpoint,
             request_timeout: fn -> request_timeout(state) end
           ),
         {:ok, timeout} <- request_timeout(state),
         capacity = Connection.capacity(conn, timeout),
         {:ok, _remaining} <- request_timeout(state) do
      {:ok, topology, conn, key, capacity}
    end
  end

  @spec load(pid(), map(), term(), map()) ::
          {:ok, term(), pid(), term(), map()} | {:error, term()}
  def load(conn, endpoint, key, state) do
    with {:ok, timeout} <- request_timeout(state),
         {:ok, topology} <- SessionBootstrap.load_topology(conn, endpoint, timeout),
         {:ok, timeout} <- request_timeout(state),
         capacity = Connection.capacity(conn, timeout),
         {:ok, _remaining} <- request_timeout(state) do
      {:ok, topology, conn, key, capacity}
    end
  end

  defp remaining_timeout(state), do: DeadlineBudget.remaining(state.deadline)
  defp request_timeout(state), do: DeadlineBudget.request_timeout(state.deadline)
  defp put_connect_timeout(endpoint, :infinity), do: endpoint

  defp put_connect_timeout(endpoint, timeout) do
    configured = Map.get(endpoint, :connect_timeout, timeout)

    connect_timeout =
      if is_integer(configured) and configured >= 0,
        do: min(configured, timeout),
        else: timeout

    Map.put(endpoint, :connect_timeout, connect_timeout)
  end
end
