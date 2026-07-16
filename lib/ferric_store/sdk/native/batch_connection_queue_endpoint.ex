defmodule FerricStore.SDK.Native.BatchConnectionQueueEndpoint do
  @moduledoc false

  @compact_min_tombstones 64

  @spec enqueue(map(), term(), reference(), reference()) :: map()
  def enqueue(by_endpoint, endpoint_key, batch_id, generation) do
    entry = {batch_id, generation}

    Map.update(
      by_endpoint,
      endpoint_key,
      %{order: :queue.in(entry, :queue.new()), size: 1, tombstones: 0},
      fn endpoint_queue ->
        %{
          endpoint_queue
          | order: :queue.in(entry, endpoint_queue.order),
            size: endpoint_queue.size + 1
        }
      end
    )
  end

  @spec remove(map(), term(), map()) :: map()
  def remove(by_endpoint, endpoint_key, entries) do
    endpoint_queue = Map.fetch!(by_endpoint, endpoint_key)
    size = endpoint_queue.size - 1

    if size == 0 do
      Map.delete(by_endpoint, endpoint_key)
    else
      endpoint_queue =
        endpoint_queue
        |> Map.put(:size, size)
        |> Map.update!(:tombstones, &(&1 + 1))
        |> compact(entries, endpoint_key)

      Map.put(by_endpoint, endpoint_key, endpoint_queue)
    end
  end

  @spec take(map(), map(), term(), pos_integer()) :: {[reference()], map(), map()}
  def take(endpoint_queue, entries, endpoint_key, limit) do
    {ids, endpoint_queue, entries} =
      take_fifo(endpoint_queue, entries, endpoint_key, limit, [])

    {ids, compact(endpoint_queue, entries, endpoint_key), entries}
  end

  defp take_fifo(endpoint_queue, entries, _endpoint_key, 0, acc),
    do: {Enum.reverse(acc), endpoint_queue, entries}

  defp take_fifo(%{size: 0} = endpoint_queue, entries, _endpoint_key, _limit, acc),
    do: {Enum.reverse(acc), endpoint_queue, entries}

  defp take_fifo(endpoint_queue, entries, endpoint_key, limit, acc) do
    case :queue.out(endpoint_queue.order) do
      {{:value, {batch_id, generation}}, order} ->
        take_entry(
          Map.get(entries, batch_id),
          batch_id,
          generation,
          %{endpoint_queue | order: order},
          entries,
          endpoint_key,
          limit,
          acc
        )

      {:empty, _order} ->
        {Enum.reverse(acc), %{endpoint_queue | size: 0, tombstones: 0}, entries}
    end
  end

  defp take_entry(
         {endpoint_key, generation},
         batch_id,
         generation,
         endpoint_queue,
         entries,
         endpoint_key,
         limit,
         acc
       ) do
    take_fifo(
      %{endpoint_queue | size: endpoint_queue.size - 1},
      Map.delete(entries, batch_id),
      endpoint_key,
      limit - 1,
      [batch_id | acc]
    )
  end

  defp take_entry(
         _stale,
         _batch_id,
         _generation,
         endpoint_queue,
         entries,
         endpoint_key,
         limit,
         acc
       ) do
    take_fifo(
      %{endpoint_queue | tombstones: max(endpoint_queue.tombstones - 1, 0)},
      entries,
      endpoint_key,
      limit,
      acc
    )
  end

  defp compact(%{tombstones: tombstones, size: size} = endpoint_queue, entries, endpoint_key) do
    if tombstones >= @compact_min_tombstones and tombstones > size do
      order =
        endpoint_queue.order
        |> :queue.to_list()
        |> Enum.filter(fn {batch_id, generation} ->
          Map.get(entries, batch_id) == {endpoint_key, generation}
        end)
        |> :queue.from_list()

      %{endpoint_queue | order: order, tombstones: 0}
    else
      endpoint_queue
    end
  end
end
