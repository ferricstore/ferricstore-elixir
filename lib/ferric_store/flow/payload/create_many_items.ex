defmodule FerricStore.Flow.Payload.CreateManyItems do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.Codec.Raw
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.CodecRuntime
  alias FerricStore.Flow.Payload.CreateManyMapItem

  @spec map(list(), module(), non_neg_integer(), DeadlineBudget.t() | nil) ::
          {:ok, non_neg_integer(), list()} | {:error, term()}
  def map(items, codec, limit, nil),
    do: map_results(items, limit, &map_item(&1, codec), nil)

  def map(items, Raw, limit, %DeadlineBudget{} = budget),
    do: map_results(items, limit, &map_item_with_budget(&1, Raw, budget), budget)

  def map(items, codec, limit, %DeadlineBudget{} = budget) do
    case CodecRuntime.run(budget, codec, fn ->
           map_results(items, limit, &map_item(&1, codec), budget)
         end) do
      {:ok, result} -> result
      {:error, :timeout} = error -> error
    end
  end

  defp map_results(items, limit, mapper, nil),
    do: BoundedList.map_result_with_count(items, limit, mapper)

  defp map_results(items, limit, mapper, %DeadlineBudget{} = budget),
    do: BoundedList.map_result_with_count(items, limit, mapper, budget)

  defp map_item(item, codec) when is_atom(codec) do
    map_item(item, fn value -> {:ok, CodecRuntime.encode(codec, value)} end)
  end

  defp map_item(id, _encode) when is_binary(id) and id != "", do: {:ok, [id, ""]}

  defp map_item({id, payload}, encode) when is_binary(id) and id != "" do
    with {:ok, encoded} <- encode.(payload), do: {:ok, [id, encoded]}
  end

  defp map_item(%{} = item, encode), do: CreateManyMapItem.map(item, encode)

  defp map_item(item, _encode), do: invalid(item)

  defp map_item_with_budget(item, codec, %DeadlineBudget{} = budget) do
    map_item(item, fn value -> CodecRuntime.encode(codec, value, budget) end)
  end

  defp invalid(item), do: {:error, {:invalid_flow_create_many_item, item}}
end
