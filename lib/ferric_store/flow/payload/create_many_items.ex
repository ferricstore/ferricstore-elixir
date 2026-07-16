defmodule FerricStore.Flow.Payload.CreateManyItems do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.Codec.Raw
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.CodecRuntime
  alias FerricStore.Types

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

  defp map_item(%{} = item, encode) when map_size(item) <= 3 do
    with {:ok, normalized} <- Types.normalize_map_keys_result(item),
         true <- only_keys?(normalized),
         id when is_binary(id) and id != "" <- Map.get(normalized, "id"),
         partition_key <- Map.get(normalized, "partition_key"),
         true <- is_nil(partition_key) or (is_binary(partition_key) and partition_key != ""),
         {:ok, payload} <- encode_optional(encode, Map.get(normalized, "payload")) do
      if partition_key,
        do: {:ok, [id, partition_key, payload]},
        else: {:ok, [id, payload]}
    else
      _invalid -> invalid(item)
    end
  end

  defp map_item(%{} = item, _encode), do: invalid(item)

  defp map_item(item, _encode), do: invalid(item)

  defp map_item_with_budget(item, codec, %DeadlineBudget{} = budget) do
    map_item(item, fn value -> CodecRuntime.encode(codec, value, budget) end)
  end

  defp encode_optional(_encode, nil), do: {:ok, ""}
  defp encode_optional(encode, value), do: encode.(value)

  defp only_keys?(map),
    do: Enum.all?(Map.keys(map), &(&1 in ["id", "partition_key", "payload"]))

  defp invalid(item), do: {:error, {:invalid_flow_create_many_item, item}}
end
