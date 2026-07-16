defmodule FerricStore.SDK.Native.ConnectionLifecycle do
  @moduledoc false

  alias FerricStore.SDK.Native.{Connection, ConnectionPool, LifecycleRegistry}

  @spec start(pid(), map()) :: {:ok, pid()} | {:error, {:connect_failed, term()}}
  def start(supervisor, endpoint) when is_pid(supervisor) and is_map(endpoint) do
    case DynamicSupervisor.start_child(supervisor, {Connection, endpoint}) do
      {:ok, connection} -> {:ok, connection}
      {:error, reason} -> {:error, {:connect_failed, reason}}
    end
  end

  @spec track(ConnectionPool.t(), LifecycleRegistry.t(), term(), pid(), map()) ::
          {:ok, ConnectionPool.t(), LifecycleRegistry.t()}
          | {:error, :connection_backpressure, ConnectionPool.t(), LifecycleRegistry.t()}
  def track(pool, registry, key, connection, capacity) when is_pid(connection) do
    already_tracked? = ConnectionPool.connection?(pool, connection)

    case ConnectionPool.track(pool, key, connection, capacity) do
      {:ok, pool} when already_tracked? ->
        {:ok, pool, registry}

      {:ok, pool} ->
        {:ok, pool, monitor_connection(registry, connection)}

      {:error, :connection_backpressure, pool} ->
        {:error, :connection_backpressure, pool, registry}
    end
  end

  @spec down(ConnectionPool.t(), pid()) :: ConnectionPool.t()
  def down(pool, connection) when is_pid(connection),
    do: ConnectionPool.remove_connection(pool, connection)

  @spec prune(ConnectionPool.t(), MapSet.t()) :: ConnectionPool.t()
  def prune(pool, keep_keys) do
    {pool, stale_connections} = ConnectionPool.prune(pool, keep_keys)
    Enum.each(stale_connections, &drain/1)
    pool
  end

  @spec retire(ConnectionPool.t(), pid()) :: ConnectionPool.t()
  def retire(pool, connection) when is_pid(connection) do
    drain(connection)
    ConnectionPool.retire_connection(pool, connection)
  end

  @spec remove(ConnectionPool.t(), pid()) :: ConnectionPool.t()
  def remove(pool, connection) when is_pid(connection),
    do: ConnectionPool.remove_connection(pool, connection)

  @spec stop_attempts(pid() | nil, [{term(), map()}]) :: :ok
  def stop_attempts(supervisor, attempts) do
    Enum.each(attempts, fn {_key, attempt} ->
      Process.demonitor(attempt.monitor, [:flush])
      stop(supervisor, attempt.starter)
    end)
  end

  @spec stop(pid() | nil, pid()) :: :ok
  def stop(supervisor, child) when is_pid(supervisor) and is_pid(child) do
    Process.unlink(child)
    _result = DynamicSupervisor.terminate_child(supervisor, child)
    :ok
  catch
    :exit, _reason -> :ok
  end

  def stop(_supervisor, _child), do: :ok

  defp monitor_connection(registry, connection) do
    monitor = Process.monitor(connection)
    LifecycleRegistry.put(registry, monitor, {:connection, connection})
  end

  defp drain(connection) do
    if Process.alive?(connection), do: Connection.drain(connection), else: :ok
  end
end
