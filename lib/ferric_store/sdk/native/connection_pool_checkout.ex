defmodule FerricStore.SDK.Native.ConnectionPoolCheckout do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionAttempts,
    ConnectionCapacity,
    ConnectionPoolRegistry,
    EndpointConnectionIndex
  }

  @spec checkout(map(), term(), term()) ::
          {:ready, pid(), map()}
          | {:waiting, map()}
          | {:start, map()}
          | {:error, :connection_backpressure, map()}
  def checkout(pool, key, waiter) do
    case Map.fetch(pool.endpoint_indexes, key) do
      :error -> checkout_uncached(pool, key, waiter)
      {:ok, index} -> checkout_cached(pool, key, index, waiter)
    end
  end

  @spec checkout_capacity(map(), term(), non_neg_integer(), term()) ::
          {:ready, pid(), map()}
          | {:waiting, map()}
          | {:start, map()}
          | {:capacity, map()}
          | {:error, :connection_backpressure, map()}
  def checkout_capacity(pool, key, lane_id, waiter)
      when is_integer(lane_id) and lane_id >= 0 do
    case reserve(pool, key, lane_id) do
      {:ok, connection, pool} -> {:ready, connection, pool}
      {:error, :missing, pool} -> checkout_uncached(pool, key, waiter)
      {:error, :capacity, pool} -> maybe_expand_for_capacity(pool, key, waiter)
    end
  end

  @spec full?(map()) :: boolean()
  def full?(pool) do
    connecting = ConnectionAttempts.size(pool.attempts) + pool.refresh_reservations

    connecting >= pool.max_connecting or
      map_size(pool.connections) + MapSet.size(pool.retiring_connections) + connecting >=
        pool.max_connections
  end

  @spec reserve(map(), term(), non_neg_integer()) ::
          {:ok, pid(), map()} | {:error, :capacity | :missing, map()}
  def reserve(pool, key, lane_id) when is_integer(lane_id) and lane_id >= 0 do
    case Map.fetch(pool.endpoint_indexes, key) do
      :error -> {:error, :missing, pool}
      {:ok, index} -> reserve_from_index(pool, key, lane_id, index)
    end
  end

  @spec update_capacity(map(), pid(), map()) :: map()
  def update_capacity(pool, connection, limits) when is_pid(connection) do
    if ConnectionPoolRegistry.connection?(pool, connection),
      do: %{pool | capacity: ConnectionCapacity.put(pool.capacity, connection, limits)},
      else: pool
  end

  @spec mark_busy(map(), pid(), non_neg_integer() | nil) :: map()
  def mark_busy(pool, connection, lane_id) when is_pid(connection) do
    pool =
      ConnectionPoolRegistry.update_load(
        pool,
        connection,
        &EndpointConnectionIndex.increment/2
      )

    if is_integer(lane_id) and lane_id >= 0,
      do: %{pool | capacity: ConnectionCapacity.reserve(pool.capacity, connection, lane_id)},
      else: pool
  end

  @spec mark_idle(map(), pid(), non_neg_integer() | nil) :: map()
  def mark_idle(pool, connection, lane_id) when is_pid(connection) do
    pool =
      ConnectionPoolRegistry.update_load(
        pool,
        connection,
        &EndpointConnectionIndex.decrement/2
      )

    if is_integer(lane_id) and lane_id >= 0,
      do: %{pool | capacity: ConnectionCapacity.release(pool.capacity, connection, lane_id)},
      else: pool
  end

  defp maybe_expand_for_capacity(pool, key, waiter) do
    index = Map.fetch!(pool.endpoint_indexes, key)

    cond do
      EndpointConnectionIndex.size(index) >= pool.connections_per_endpoint ->
        {:capacity, pool}

      attempt = ConnectionAttempts.fetch(pool.attempts, key) ->
        {:waiting, add_attempt_waiter(pool, key, attempt, waiter)}

      full?(pool) ->
        {:capacity, pool}

      true ->
        {:start, pool}
    end
  end

  defp checkout_cached(pool, key, index, waiter) do
    cond do
      not should_expand?(pool, index) ->
        ready_connection(pool, key, index, waiter)

      attempt = ConnectionAttempts.fetch(pool.attempts, key) ->
        {:waiting, add_attempt_waiter(pool, key, attempt, waiter)}

      full?(pool) ->
        ready_connection(pool, key, index, waiter)

      true ->
        {:start, pool}
    end
  end

  defp should_expand?(pool, index) do
    EndpointConnectionIndex.size(index) < pool.connections_per_endpoint and
      EndpointConnectionIndex.min_load(index) > 0
  end

  defp ready_connection(pool, key, index, waiter) do
    {connection_id, index} = EndpointConnectionIndex.checkout(index)
    connection = Map.fetch!(pool.connections, connection_id)

    if Process.alive?(connection) do
      pool = %{pool | endpoint_indexes: Map.put(pool.endpoint_indexes, key, index)}
      {:ready, connection, pool}
    else
      pool
      |> ConnectionPoolRegistry.remove(connection)
      |> checkout(key, waiter)
    end
  end

  defp checkout_uncached(pool, key, waiter) do
    case ConnectionAttempts.fetch(pool.attempts, key) do
      attempt when is_map(attempt) ->
        {:waiting, add_attempt_waiter(pool, key, attempt, waiter)}

      nil ->
        if full?(pool),
          do: {:error, :connection_backpressure, pool},
          else: {:start, pool}
    end
  end

  defp reserve_from_index(pool, key, lane_id, index) do
    available? = fn connection_id, load ->
      connection = Map.fetch!(pool.connections, connection_id)

      Process.alive?(connection) and
        ConnectionCapacity.available?(pool.capacity, connection, load, lane_id)
    end

    case EndpointConnectionIndex.checkout_available(index, available?) do
      :error ->
        {:error, :capacity, pool}

      {:ok, connection_id, index} ->
        connection = Map.fetch!(pool.connections, connection_id)
        index = EndpointConnectionIndex.increment(index, connection_id)

        pool = %{
          pool
          | endpoint_indexes: Map.put(pool.endpoint_indexes, key, index),
            capacity: ConnectionCapacity.reserve(pool.capacity, connection, lane_id)
        }

        {:ok, connection, pool}
    end
  end

  defp add_attempt_waiter(pool, key, attempt, waiter) do
    if MapSet.member?(attempt.waiters, waiter) do
      pool
    else
      %{pool | attempts: ConnectionAttempts.add_waiter(pool.attempts, key, waiter)}
    end
  end
end
