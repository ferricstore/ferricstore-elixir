defmodule FerricStore.Protocol.FlowCreateBatchItems do
  @moduledoc false

  alias FerricStore.Protocol.FlowBatchFields

  def create(items, nil, item_count),
    do: encode(items, nil, [], nil, 0, item_count)

  def create(items, partition_key, item_count) when is_binary(partition_key),
    do: encode(items, partition_key, [], :regular, 0, item_count)

  def ids(ids, nil), do: encode_ids(ids, 0x90)
  def ids(ids, partition_key) when is_binary(partition_key), do: encode_ids(ids, 0x96)

  defp encode([], nil, encoded, nil, count, _item_count),
    do: {:ok, Enum.reverse(encoded), 0x90, count}

  defp encode([], nil, encoded, :regular, count, _item_count),
    do: {:ok, Enum.reverse(encoded), 0x90, count}

  defp encode([], nil, encoded, :mixed, count, _item_count),
    do: {:ok, Enum.reverse(encoded), 0x9E, count}

  defp encode([], partition_key, encoded, :regular, count, _item_count)
       when is_binary(partition_key),
       do: {:ok, Enum.reverse(encoded), 0x96, count}

  defp encode([_item | _items], _key, _encoded, _mode, count, count), do: :error

  defp encode([[id, payload] | items], partition_key, encoded, mode, count, item_count)
       when is_binary(id) and is_binary(payload) and mode in [nil, :regular] do
    encode(
      items,
      partition_key,
      [[FlowBatchFields.binary(id), FlowBatchFields.binary(payload)] | encoded],
      :regular,
      count + 1,
      item_count
    )
  end

  defp encode(
         [[id, partition_key, payload] | items],
         nil,
         encoded,
         mode,
         count,
         item_count
       )
       when is_binary(id) and is_binary(partition_key) and is_binary(payload) and
              mode in [nil, :mixed] do
    encode(
      items,
      nil,
      [
        [
          FlowBatchFields.binary(id),
          FlowBatchFields.binary(partition_key),
          FlowBatchFields.binary(payload)
        ]
        | encoded
      ],
      :mixed,
      count + 1,
      item_count
    )
  end

  defp encode(_items, _key, _encoded, _mode, _count, _item_count), do: :error

  defp encode_ids(ids, tag) do
    ids
    |> Enum.reduce_while([], fn
      id, acc when is_binary(id) ->
        {:cont, [[FlowBatchFields.binary(id), <<0::32>>] | acc]}

      _id, _acc ->
        {:halt, :error}
    end)
    |> case do
      :error -> :error
      encoded -> {:ok, Enum.reverse(encoded), tag}
    end
  end
end
