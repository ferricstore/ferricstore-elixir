defmodule FerricStore.SDK.Native.BatchConnectionQueue do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchConnectionQueueEndpoint, BatchConnectionQueueOrder}

  defstruct order: {[], []}, entries: %{}, by_endpoint: %{}, tombstones: 0

  @type endpoint_queue :: %{
          order: :queue.queue({reference(), reference()}),
          size: non_neg_integer(),
          tombstones: non_neg_integer()
        }

  @type t :: %__MODULE__{
          order: :queue.queue({reference(), reference()}),
          entries: %{reference() => {term(), reference()}},
          by_endpoint: %{term() => endpoint_queue()},
          tombstones: non_neg_integer()
        }

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{entries: entries}), do: map_size(entries)

  @spec enqueue(t(), reference(), term()) :: t()
  def enqueue(%__MODULE__{entries: entries} = queue, batch_id, endpoint_key)
      when is_reference(batch_id) do
    case Map.fetch(entries, batch_id) do
      {:ok, {^endpoint_key, _generation}} ->
        queue

      {:ok, {_old_endpoint_key, _generation}} ->
        queue |> delete(batch_id) |> enqueue(batch_id, endpoint_key)

      :error ->
        generation = make_ref()

        %{
          queue
          | order: :queue.in({batch_id, generation}, queue.order),
            entries: Map.put(entries, batch_id, {endpoint_key, generation}),
            by_endpoint:
              BatchConnectionQueueEndpoint.enqueue(
                queue.by_endpoint,
                endpoint_key,
                batch_id,
                generation
              )
        }
    end
  end

  @spec delete(t(), reference()) :: t()
  def delete(%__MODULE__{entries: entries} = queue, batch_id) when is_reference(batch_id) do
    case Map.pop(entries, batch_id) do
      {nil, _entries} ->
        queue

      {{_endpoint_key, _generation}, entries} when map_size(entries) == 0 ->
        %__MODULE__{}

      {{endpoint_key, _generation}, entries} ->
        queue
        |> Map.put(:entries, entries)
        |> Map.put(
          :by_endpoint,
          BatchConnectionQueueEndpoint.remove(queue.by_endpoint, endpoint_key, entries)
        )
        |> Map.update!(:tombstones, &(&1 + 1))
        |> BatchConnectionQueueOrder.compact()
    end
  end

  @spec endpoint_size(t(), term()) :: non_neg_integer()
  def endpoint_size(%__MODULE__{by_endpoint: by_endpoint}, endpoint_key) do
    case Map.get(by_endpoint, endpoint_key) do
      %{size: size} -> size
      nil -> 0
    end
  end

  @spec take_endpoint(t(), term(), pos_integer()) :: {[reference()], t()}
  def take_endpoint(%__MODULE__{} = queue, endpoint_key, limit)
      when is_integer(limit) and limit > 0 do
    case Map.get(queue.by_endpoint, endpoint_key) do
      nil ->
        {[], queue}

      endpoint_queue ->
        {ids, endpoint_queue, entries} =
          BatchConnectionQueueEndpoint.take(
            endpoint_queue,
            queue.entries,
            endpoint_key,
            limit
          )

        after_endpoint_take(queue, endpoint_key, ids, endpoint_queue, entries)
    end
  end

  @spec out(t()) :: {{:value, reference()}, t()} | {:empty, t()}
  def out(%__MODULE__{entries: entries}) when map_size(entries) == 0,
    do: {:empty, %__MODULE__{}}

  def out(%__MODULE__{} = queue) do
    case BatchConnectionQueueOrder.pop(queue.order, queue.entries, queue.tombstones) do
      :empty ->
        {:empty, %__MODULE__{}}

      {:value, batch_id, _endpoint_key, _order, entries, _tombstones}
      when map_size(entries) == 0 ->
        {{:value, batch_id}, %__MODULE__{}}

      {:value, batch_id, endpoint_key, order, entries, tombstones} ->
        by_endpoint =
          BatchConnectionQueueEndpoint.remove(queue.by_endpoint, endpoint_key, entries)

        {{:value, batch_id},
         %{
           queue
           | order: order,
             entries: entries,
             by_endpoint: by_endpoint,
             tombstones: tombstones
         }}
    end
  end

  defp after_endpoint_take(_queue, _key, ids, _endpoint_queue, entries)
       when map_size(entries) == 0,
       do: {ids, %__MODULE__{}}

  defp after_endpoint_take(queue, endpoint_key, ids, endpoint_queue, entries) do
    by_endpoint =
      if endpoint_queue.size == 0,
        do: Map.delete(queue.by_endpoint, endpoint_key),
        else: Map.put(queue.by_endpoint, endpoint_key, endpoint_queue)

    updated =
      BatchConnectionQueueOrder.compact(%{
        queue
        | entries: entries,
          by_endpoint: by_endpoint,
          tombstones: queue.tombstones + length(ids)
      })

    {ids, updated}
  end
end
