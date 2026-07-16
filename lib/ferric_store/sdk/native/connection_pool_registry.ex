defmodule FerricStore.SDK.Native.ConnectionPoolRegistry do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionAttempts,
    ConnectionCapacity,
    EndpointConnectionIndex
  }

  @spec track(map(), term(), pid(), map()) ::
          {:ok, map()} | {:error, :connection_backpressure, map()}
  def track(pool, key, connection, limits) when is_pid(connection) do
    endpoint_index = Map.get(pool.endpoint_indexes, key, EndpointConnectionIndex.new())

    cond do
      Map.has_key?(pool.connection_keys_by_pid, connection) ->
        {:ok, pool}

      EndpointConnectionIndex.size(endpoint_index) >= pool.connections_per_endpoint ->
        {:error, :connection_backpressure, pool}

      at_connection_limit?(pool) ->
        {:error, :connection_backpressure, pool}

      true ->
        connection_id = available_connection_id(pool, key)
        endpoint_index = EndpointConnectionIndex.put(endpoint_index, connection_id)

        {:ok,
         %{
           pool
           | connections: Map.put(pool.connections, connection_id, connection),
             connection_keys_by_pid:
               Map.put(pool.connection_keys_by_pid, connection, connection_id),
             endpoint_keys_by_connection:
               Map.put(pool.endpoint_keys_by_connection, connection_id, key),
             endpoint_indexes: Map.put(pool.endpoint_indexes, key, endpoint_index),
             capacity: ConnectionCapacity.put(pool.capacity, connection, limits)
         }}
    end
  end

  @spec fetch_connection(map(), term()) :: {:ok, pid()} | :error
  def fetch_connection(pool, key) do
    case Map.fetch(pool.endpoint_indexes, key) do
      {:ok, index} -> Map.fetch(pool.connections, EndpointConnectionIndex.peek(index))
      :error -> :error
    end
  end

  @spec get_connection(map(), term()) :: pid() | nil
  def get_connection(pool, key) do
    case fetch_connection(pool, key) do
      {:ok, connection} -> connection
      :error -> nil
    end
  end

  @spec endpoint_connections(map()) :: map()
  def endpoint_connections(pool) do
    Map.new(pool.endpoint_indexes, fn {endpoint_key, index} ->
      connection_id = EndpointConnectionIndex.peek(index)
      {endpoint_key, Map.fetch!(pool.connections, connection_id)}
    end)
  end

  @spec connection?(map(), pid()) :: boolean()
  def connection?(pool, connection) when is_pid(connection),
    do: Map.has_key?(pool.connection_keys_by_pid, connection)

  @spec endpoint_key(map(), pid()) :: {:ok, term()} | :error
  def endpoint_key(pool, connection) when is_pid(connection) do
    with {:ok, connection_id} <- Map.fetch(pool.connection_keys_by_pid, connection) do
      Map.fetch(pool.endpoint_keys_by_connection, connection_id)
    end
  end

  @spec retire(map(), pid()) :: map()
  def retire(pool, connection) when is_pid(connection) do
    if connection?(pool, connection) do
      pool = detach(pool, connection)
      %{pool | retiring_connections: MapSet.put(pool.retiring_connections, connection)}
    else
      pool
    end
  end

  @spec remove(map(), pid()) :: map()
  def remove(pool, connection) when is_pid(connection) do
    if connection?(pool, connection) do
      detach(pool, connection)
    else
      %{pool | retiring_connections: MapSet.delete(pool.retiring_connections, connection)}
    end
  end

  @spec prune(map(), MapSet.t()) :: {map(), [pid()]}
  def prune(pool, keep_keys) do
    Enum.reduce(pool.connection_keys_by_pid, {pool, []}, fn {connection, connection_id},
                                                            {pool, stale} ->
      endpoint_key = Map.fetch!(pool.endpoint_keys_by_connection, connection_id)

      if MapSet.member?(keep_keys, endpoint_key) and Process.alive?(connection) do
        {pool, stale}
      else
        {prune_connection(pool, connection), [connection | stale]}
      end
    end)
  end

  @spec update_load(map(), pid(), (term(), term() -> term())) :: map()
  def update_load(pool, connection, updater) when is_pid(connection) do
    case Map.fetch(pool.connection_keys_by_pid, connection) do
      :error ->
        pool

      {:ok, connection_id} ->
        endpoint_key = Map.fetch!(pool.endpoint_keys_by_connection, connection_id)
        endpoint_index = Map.fetch!(pool.endpoint_indexes, endpoint_key)
        endpoint_index = updater.(endpoint_index, connection_id)
        %{pool | endpoint_indexes: Map.put(pool.endpoint_indexes, endpoint_key, endpoint_index)}
    end
  end

  defp detach(pool, connection) do
    case Map.pop(pool.connection_keys_by_pid, connection) do
      {nil, _index} ->
        pool

      {connection_id, connection_keys_by_pid} ->
        endpoint_key = Map.fetch!(pool.endpoint_keys_by_connection, connection_id)

        endpoint_index =
          pool.endpoint_indexes
          |> Map.fetch!(endpoint_key)
          |> EndpointConnectionIndex.delete(connection_id)

        endpoint_indexes =
          put_or_delete_index(pool.endpoint_indexes, endpoint_key, endpoint_index)

        %{
          pool
          | connections: Map.delete(pool.connections, connection_id),
            connection_keys_by_pid: connection_keys_by_pid,
            endpoint_keys_by_connection:
              Map.delete(pool.endpoint_keys_by_connection, connection_id),
            endpoint_indexes: endpoint_indexes,
            capacity: ConnectionCapacity.delete(pool.capacity, connection)
        }
    end
  end

  defp put_or_delete_index(indexes, key, index) do
    if EndpointConnectionIndex.empty?(index),
      do: Map.delete(indexes, key),
      else: Map.put(indexes, key, index)
  end

  defp prune_connection(pool, connection) do
    if Process.alive?(connection), do: retire(pool, connection), else: remove(pool, connection)
  end

  defp available_connection_id(pool, key) do
    if Map.has_key?(pool.connections, key), do: {key, make_ref()}, else: key
  end

  defp at_connection_limit?(pool) do
    map_size(pool.connections) + MapSet.size(pool.retiring_connections) +
      ConnectionAttempts.size(pool.attempts) + pool.refresh_reservations >= pool.max_connections
  end
end
