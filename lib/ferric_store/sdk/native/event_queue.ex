defmodule FerricStore.SDK.Native.EventQueue do
  @moduledoc false

  @compact_min_tombstones 64

  defstruct order: {[], []}, calls: %{}, tombstones: 0

  @type event_call :: %{required(:id) => reference()}
  @type t :: %__MODULE__{
          order: :queue.queue(reference()),
          calls: %{reference() => event_call()},
          tombstones: non_neg_integer()
        }

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{calls: calls}), do: map_size(calls)

  @spec fetch(t(), reference()) :: event_call() | nil
  def fetch(%__MODULE__{calls: calls}, event_call_id), do: Map.get(calls, event_call_id)

  @spec enqueue(t(), event_call()) :: t()
  def enqueue(%__MODULE__{calls: calls} = queue, event_call) do
    if Map.has_key?(calls, event_call.id) do
      queue
    else
      %{
        queue
        | order: :queue.in(event_call.id, queue.order),
          calls: Map.put(calls, event_call.id, event_call)
      }
    end
  end

  @spec pop(t(), reference()) :: {event_call() | nil, t()}
  def pop(%__MODULE__{calls: calls} = queue, event_call_id) do
    case Map.pop(calls, event_call_id) do
      {nil, _calls} ->
        {nil, queue}

      {event_call, calls} when map_size(calls) == 0 ->
        {event_call, %__MODULE__{}}

      {event_call, calls} ->
        next_queue = %{queue | calls: calls, tombstones: queue.tombstones + 1}
        {event_call, maybe_compact(next_queue)}
    end
  end

  @spec out(t()) :: {{:value, event_call()}, t()} | {:empty, t()}
  def out(%__MODULE__{calls: calls}) when map_size(calls) == 0,
    do: {:empty, %__MODULE__{}}

  def out(%__MODULE__{order: order, calls: calls, tombstones: tombstones} = queue) do
    case :queue.out(order) do
      {{:value, event_call_id}, order} ->
        pop_ordered_call(queue, calls, event_call_id, order, tombstones)

      {:empty, _order} ->
        {:empty, %__MODULE__{}}
    end
  end

  defp pop_ordered_call(queue, calls, event_call_id, order, tombstones) do
    case Map.pop(calls, event_call_id) do
      {nil, calls} ->
        out(%{queue | order: order, calls: calls, tombstones: tombstones - 1})

      {event_call, calls} ->
        {{:value, event_call}, after_ordered_pop(queue, calls, order)}
    end
  end

  defp after_ordered_pop(_queue, calls, _order) when map_size(calls) == 0,
    do: %__MODULE__{}

  defp after_ordered_pop(queue, calls, order),
    do: %{queue | order: order, calls: calls}

  @spec values(t()) :: [event_call()]
  def values(%__MODULE__{calls: calls}), do: Map.values(calls)

  defp maybe_compact(%__MODULE__{calls: calls, tombstones: tombstones} = queue)
       when tombstones >= @compact_min_tombstones and tombstones > map_size(calls) do
    order =
      queue.order
      |> :queue.to_list()
      |> Enum.filter(&Map.has_key?(calls, &1))
      |> :queue.from_list()

    %{queue | order: order, tombstones: 0}
  end

  defp maybe_compact(queue), do: queue
end
