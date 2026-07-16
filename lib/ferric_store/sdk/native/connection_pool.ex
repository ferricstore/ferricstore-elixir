defmodule FerricStore.SDK.Native.ConnectionPool do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionAttempts,
    ConnectionCapacity,
    ConnectionPoolCheckout,
    ConnectionPoolRefreshCapacity,
    ConnectionPoolRegistry,
    EndpointConnectionIndex
  }

  @enforce_keys [:max_connections, :max_connecting]
  defstruct connections: %{},
            connection_keys_by_pid: %{},
            endpoint_keys_by_connection: %{},
            endpoint_indexes: %{},
            retiring_connections: MapSet.new(),
            attempts: %ConnectionAttempts{},
            capacity: %ConnectionCapacity{},
            refresh_reservations: 0,
            max_connections: nil,
            max_connecting: nil,
            connections_per_endpoint: 1

  @type key :: term()
  @type attempt :: %{required(:waiters) => MapSet.t()}
  @type t :: %__MODULE__{
          connections: %{optional(key()) => pid()},
          connection_keys_by_pid: %{optional(pid()) => key()},
          endpoint_keys_by_connection: %{optional(key()) => key()},
          endpoint_indexes: %{optional(key()) => EndpointConnectionIndex.t()},
          retiring_connections: MapSet.t(pid()),
          attempts: ConnectionAttempts.t(),
          capacity: ConnectionCapacity.t(),
          refresh_reservations: non_neg_integer(),
          max_connections: pos_integer(),
          max_connecting: pos_integer(),
          connections_per_endpoint: pos_integer()
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    struct!(__MODULE__,
      max_connections: Keyword.fetch!(opts, :max_connections),
      max_connecting: Keyword.fetch!(opts, :max_connecting),
      connections_per_endpoint: Keyword.get(opts, :connections_per_endpoint, 1)
    )
  end

  @spec checkout(t(), key(), term()) ::
          {:ready, pid(), t()}
          | {:waiting, t()}
          | {:start, t()}
          | {:error, :connection_backpressure, t()}
  def checkout(%__MODULE__{} = pool, key, waiter),
    do: ConnectionPoolCheckout.checkout(pool, key, waiter)

  @spec checkout_capacity(t(), key(), non_neg_integer(), term()) ::
          {:ready, pid(), t()}
          | {:waiting, t()}
          | {:start, t()}
          | {:capacity, t()}
          | {:error, :connection_backpressure, t()}
  def checkout_capacity(%__MODULE__{} = pool, key, lane_id, waiter),
    do: ConnectionPoolCheckout.checkout_capacity(pool, key, lane_id, waiter)

  @spec full?(t()) :: boolean()
  def full?(%__MODULE__{} = pool), do: ConnectionPoolCheckout.full?(pool)

  @spec reserve_refresh(t(), boolean()) ::
          {:ok, :new | :replacement, t()} | {:error, :connection_backpressure, t()}
  def reserve_refresh(%__MODULE__{} = pool, replacement_available?)
      when is_boolean(replacement_available?) do
    ConnectionPoolRefreshCapacity.reserve(pool, replacement_available?)
  end

  @spec release_refresh(t()) :: t()
  def release_refresh(%__MODULE__{} = pool), do: ConnectionPoolRefreshCapacity.release(pool)

  @spec put_attempt(t(), key(), attempt()) :: t()
  def put_attempt(%__MODULE__{} = pool, key, attempt),
    do: %{pool | attempts: ConnectionAttempts.put(pool.attempts, key, attempt)}

  @spec pop_attempt(t(), key()) :: {attempt() | nil, t()}
  def pop_attempt(%__MODULE__{} = pool, key) do
    {attempt, attempts} = ConnectionAttempts.pop(pool.attempts, key)
    {attempt, %{pool | attempts: attempts}}
  end

  @spec fetch_attempt(t(), key()) :: attempt() | nil
  def fetch_attempt(%__MODULE__{} = pool, key), do: ConnectionAttempts.fetch(pool.attempts, key)

  @spec fetch_attempt!(t(), key()) :: attempt()
  def fetch_attempt!(%__MODULE__{} = pool, key), do: ConnectionAttempts.fetch!(pool.attempts, key)

  @spec remove_waiter(t(), key(), term()) ::
          {:missing, t()} | {:remaining, t()} | {:empty, attempt(), t()}
  def remove_waiter(%__MODULE__{} = pool, key, waiter) do
    case ConnectionAttempts.remove_waiter(pool.attempts, key, waiter) do
      {:missing, attempts} -> {:missing, %{pool | attempts: attempts}}
      {:remaining, attempts} -> {:remaining, %{pool | attempts: attempts}}
      {:empty, attempt, attempts} -> {:empty, attempt, %{pool | attempts: attempts}}
    end
  end

  @spec remove_batch_waiters(t(), reference()) :: {[{key(), attempt()}], t()}
  def remove_batch_waiters(%__MODULE__{} = pool, batch_id) when is_reference(batch_id) do
    {emptied, attempts} = ConnectionAttempts.remove_batch_waiters(pool.attempts, batch_id)
    {emptied, %{pool | attempts: attempts}}
  end

  @spec track(t(), key(), pid(), map()) :: {:ok, t()} | {:error, :connection_backpressure, t()}
  def track(%__MODULE__{} = pool, key, connection, limits \\ %{}),
    do: ConnectionPoolRegistry.track(pool, key, connection, limits)

  @spec fetch_connection(t(), key()) :: {:ok, pid()} | :error
  def fetch_connection(%__MODULE__{} = pool, key),
    do: ConnectionPoolRegistry.fetch_connection(pool, key)

  @spec get_connection(t(), key()) :: pid() | nil
  def get_connection(%__MODULE__{} = pool, key),
    do: ConnectionPoolRegistry.get_connection(pool, key)

  @spec connections(t()) :: %{optional(key()) => pid()}
  def connections(%__MODULE__{} = pool), do: pool.connections

  @spec connection_values(t()) :: [pid()]
  def connection_values(%__MODULE__{} = pool), do: Map.values(pool.connections)

  @spec endpoint_connections(t()) :: %{optional(key()) => pid()}
  def endpoint_connections(%__MODULE__{} = pool),
    do: ConnectionPoolRegistry.endpoint_connections(pool)

  @spec connection?(t(), pid()) :: boolean()
  def connection?(%__MODULE__{} = pool, connection),
    do: ConnectionPoolRegistry.connection?(pool, connection)

  @spec endpoint_key(t(), pid()) :: {:ok, key()} | :error
  def endpoint_key(%__MODULE__{} = pool, connection),
    do: ConnectionPoolRegistry.endpoint_key(pool, connection)

  @spec reserve(t(), key(), non_neg_integer()) ::
          {:ok, pid(), t()} | {:error, :capacity | :missing, t()}
  def reserve(%__MODULE__{} = pool, key, lane_id),
    do: ConnectionPoolCheckout.reserve(pool, key, lane_id)

  @spec update_capacity(t(), pid(), map()) :: t()
  def update_capacity(%__MODULE__{} = pool, connection, limits),
    do: ConnectionPoolCheckout.update_capacity(pool, connection, limits)

  @spec mark_busy(t(), pid(), non_neg_integer() | nil) :: t()
  def mark_busy(%__MODULE__{} = pool, connection, lane_id \\ nil),
    do: ConnectionPoolCheckout.mark_busy(pool, connection, lane_id)

  @spec mark_idle(t(), pid(), non_neg_integer() | nil) :: t()
  def mark_idle(%__MODULE__{} = pool, connection, lane_id \\ nil),
    do: ConnectionPoolCheckout.mark_idle(pool, connection, lane_id)

  @spec contains?(t(), key()) :: boolean()
  def contains?(%__MODULE__{} = pool, key),
    do:
      Map.has_key?(pool.endpoint_indexes, key) or
        ConnectionAttempts.contains?(pool.attempts, key)

  @spec slot_available?(t(), key()) :: boolean()
  def slot_available?(%__MODULE__{} = pool, key),
    do: contains?(pool, key) or not full?(pool)

  @spec connecting_count(t()) :: non_neg_integer()
  def connecting_count(%__MODULE__{} = pool), do: ConnectionAttempts.size(pool.attempts)

  @spec retiring?(t(), pid()) :: boolean()
  def retiring?(%__MODULE__{} = pool, connection),
    do: MapSet.member?(pool.retiring_connections, connection)

  @spec retire_connection(t(), pid()) :: t()
  def retire_connection(%__MODULE__{} = pool, connection),
    do: ConnectionPoolRegistry.retire(pool, connection)

  @spec remove_connection(t(), pid()) :: t()
  def remove_connection(%__MODULE__{} = pool, connection),
    do: ConnectionPoolRegistry.remove(pool, connection)

  @spec prune(t(), MapSet.t()) :: {t(), [pid()]}
  def prune(%__MODULE__{} = pool, keep_keys), do: ConnectionPoolRegistry.prune(pool, keep_keys)
end
