defmodule FerricStore.SDK.Native.BatchConnectionQueueOrder do
  @moduledoc false

  @compact_min_tombstones 64

  @spec pop(:queue.queue(), map(), non_neg_integer()) ::
          :empty | {:value, reference(), term(), :queue.queue(), map(), non_neg_integer()}
  def pop(order, entries, tombstones) do
    case :queue.out(order) do
      {{:value, {batch_id, generation}}, order} ->
        case Map.get(entries, batch_id) do
          {endpoint_key, ^generation} ->
            {:value, batch_id, endpoint_key, order, Map.delete(entries, batch_id), tombstones}

          _missing_or_stale ->
            pop(order, entries, max(tombstones - 1, 0))
        end

      {:empty, _order} ->
        :empty
    end
  end

  @spec compact(map()) :: map()
  def compact(%{entries: entries, tombstones: tombstones} = queue) do
    if tombstones >= @compact_min_tombstones and tombstones > map_size(entries) do
      order =
        queue.order
        |> :queue.to_list()
        |> Enum.filter(&current_entry?(entries, &1))
        |> :queue.from_list()

      %{queue | order: order, tombstones: 0}
    else
      queue
    end
  end

  defp current_entry?(entries, {batch_id, generation}) do
    case Map.get(entries, batch_id) do
      {_endpoint_key, ^generation} -> true
      _missing_or_stale -> false
    end
  end
end
