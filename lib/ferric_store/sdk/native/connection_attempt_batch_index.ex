defmodule FerricStore.SDK.Native.ConnectionAttemptBatchIndex do
  @moduledoc false

  @spec put(map(), term(), term()) :: map()
  def put(index, key, {:batch, batch_id, _group} = waiter) when is_reference(batch_id) do
    Map.update(index, batch_id, %{key => MapSet.new([waiter])}, fn waiters_by_key ->
      Map.update(waiters_by_key, key, MapSet.new([waiter]), &MapSet.put(&1, waiter))
    end)
  end

  def put(index, _key, _waiter), do: index

  @spec delete(map(), term(), term()) :: map()
  def delete(index, key, {:batch, batch_id, _group} = waiter) when is_reference(batch_id) do
    case Map.fetch(index, batch_id) do
      :error ->
        index

      {:ok, waiters_by_key} ->
        waiters = waiters_by_key |> Map.get(key, MapSet.new()) |> MapSet.delete(waiter)

        waiters_by_key =
          if MapSet.size(waiters) == 0,
            do: Map.delete(waiters_by_key, key),
            else: Map.put(waiters_by_key, key, waiters)

        if map_size(waiters_by_key) == 0,
          do: Map.delete(index, batch_id),
          else: Map.put(index, batch_id, waiters_by_key)
    end
  end

  def delete(index, _key, _waiter), do: index
end
