defmodule FerricStore.SDK.Native.EndpointConnectionIndex do
  @moduledoc false

  @enforce_keys [:ordered]
  defstruct entries: %{}, ordered: nil, next_order: 0

  @type connection_id :: term()
  @type entry :: {non_neg_integer(), non_neg_integer()}
  @type t :: %__MODULE__{
          entries: %{optional(connection_id()) => entry()},
          ordered: term(),
          next_order: non_neg_integer()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{ordered: :gb_sets.empty()}

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{entries: entries}), do: map_size(entries)

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = index), do: size(index) == 0

  @spec put(t(), connection_id()) :: t()
  def put(%__MODULE__{} = index, connection_id) do
    if Map.has_key?(index.entries, connection_id) do
      index
    else
      order = index.next_order
      entry = {0, order, connection_id}

      %{
        index
        | entries: Map.put(index.entries, connection_id, {0, order}),
          ordered: :gb_sets.add_element(entry, index.ordered),
          next_order: order + 1
      }
    end
  end

  @spec delete(t(), connection_id()) :: t()
  def delete(%__MODULE__{} = index, connection_id) do
    case Map.pop(index.entries, connection_id) do
      {nil, _entries} ->
        index

      {{load, order}, entries} ->
        %{
          index
          | entries: entries,
            ordered: :gb_sets.delete_any({load, order, connection_id}, index.ordered)
        }
    end
  end

  @spec checkout(t()) :: {connection_id(), t()}
  def checkout(%__MODULE__{} = index) do
    {load, order, connection_id} = :gb_sets.smallest(index.ordered)

    {connection_id, rotate(index, load, order, connection_id)}
  end

  @spec checkout_available(t(), (connection_id(), non_neg_integer() -> boolean())) ::
          {:ok, connection_id(), t()} | :error
  def checkout_available(%__MODULE__{} = index, available?) when is_function(available?, 2) do
    case find_available(:gb_sets.iterator(index.ordered), available?) do
      :error ->
        :error

      {:ok, load, order, connection_id} ->
        {:ok, connection_id, rotate(index, load, order, connection_id)}
    end
  end

  defp rotate(index, load, order, connection_id) do
    if size(index) == 1 do
      index
    else
      next_order = index.next_order

      ordered = :gb_sets.delete_any({load, order, connection_id}, index.ordered)
      ordered = :gb_sets.add_element({load, next_order, connection_id}, ordered)

      %{
        index
        | entries: Map.put(index.entries, connection_id, {load, next_order}),
          ordered: ordered,
          next_order: next_order + 1
      }
    end
  end

  defp find_available(iterator, available?) do
    case :gb_sets.next(iterator) do
      :none ->
        :error

      {{load, order, connection_id}, iterator} ->
        if available?.(connection_id, load),
          do: {:ok, load, order, connection_id},
          else: find_available(iterator, available?)
    end
  end

  @spec peek(t()) :: connection_id()
  def peek(%__MODULE__{} = index) do
    {_load, _order, connection_id} = :gb_sets.smallest(index.ordered)
    connection_id
  end

  @spec min_load(t()) :: non_neg_integer()
  def min_load(%__MODULE__{} = index) do
    {load, _order, _connection_id} = :gb_sets.smallest(index.ordered)
    load
  end

  @spec increment(t(), connection_id()) :: t()
  def increment(%__MODULE__{} = index, connection_id),
    do: update_load(index, connection_id, &(&1 + 1))

  @spec decrement(t(), connection_id()) :: t()
  def decrement(%__MODULE__{} = index, connection_id),
    do: update_load(index, connection_id, &max(&1 - 1, 0))

  defp update_load(index, connection_id, updater) do
    {load, order} = Map.fetch!(index.entries, connection_id)
    next_load = updater.(load)

    if next_load == load do
      index
    else
      ordered = :gb_sets.delete_any({load, order, connection_id}, index.ordered)
      ordered = :gb_sets.add_element({next_load, order, connection_id}, ordered)

      %{
        index
        | entries: Map.put(index.entries, connection_id, {next_load, order}),
          ordered: ordered
      }
    end
  end
end
