defmodule FerricStore.SDK.Native.ConnectionPoolRefreshCapacity do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionAttempts

  @spec reserve(map(), boolean()) ::
          {:ok, :new | :replacement, map()} | {:error, :connection_backpressure, map()}
  def reserve(pool, replacement_available?) do
    connecting = ConnectionAttempts.size(pool.attempts) + pool.refresh_reservations
    total = map_size(pool.connections) + MapSet.size(pool.retiring_connections) + connecting

    cond do
      connecting >= pool.max_connecting -> {:error, :connection_backpressure, pool}
      total < pool.max_connections -> reserve_slot(pool, :new)
      replacement_available? -> reserve_slot(pool, :replacement)
      true -> {:error, :connection_backpressure, pool}
    end
  end

  @spec release(map()) :: map()
  def release(%{refresh_reservations: reservations} = pool) when reservations > 0,
    do: %{pool | refresh_reservations: reservations - 1}

  def release(pool), do: pool

  defp reserve_slot(pool, mode) do
    {:ok, mode, %{pool | refresh_reservations: pool.refresh_reservations + 1}}
  end
end
