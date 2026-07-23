defmodule FerricStore.Protocol.ValueSizer do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain,
    only: [is_signed_64_integer: 1, is_unsigned_64_integer: 1]

  alias FerricStore.Protocol.MapKey
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  @spec encoded_size(term(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, :too_large}
  def encoded_size(value, max_bytes) when is_integer(max_bytes) and max_bytes >= 0 do
    case do_encoded_size(value, 0, max_bytes) do
      {:ok, remaining} -> {:ok, max_bytes - remaining}
      :too_large -> {:error, :too_large}
    end
  end

  defp do_encoded_size(nil, _depth, remaining), do: reserve_bytes(remaining, 1)
  defp do_encoded_size(true, _depth, remaining), do: reserve_bytes(remaining, 1)
  defp do_encoded_size(false, _depth, remaining), do: reserve_bytes(remaining, 1)

  defp do_encoded_size(value, _depth, remaining) when is_signed_64_integer(value),
    do: reserve_bytes(remaining, 9)

  defp do_encoded_size(value, _depth, remaining) when is_unsigned_64_integer(value),
    do: reserve_bytes(remaining, 9)

  defp do_encoded_size(value, _depth, _remaining) when is_integer(value) do
    raise ArgumentError,
          "integer #{value} is outside the signed or unsigned 64-bit native protocol domain"
  end

  defp do_encoded_size(value, _depth, remaining) when is_binary(value),
    do: reserve_bytes(remaining, byte_size(value) + 5)

  defp do_encoded_size(value, depth, remaining) when is_atom(value),
    do: value |> Atom.to_string() |> do_encoded_size(depth, remaining)

  defp do_encoded_size(value, _depth, remaining) when is_float(value),
    do: reserve_bytes(remaining, 9)

  defp do_encoded_size(value, depth, remaining) when is_list(value) do
    validate_value_depth!(depth)

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      encoded_list_size(value, depth + 1, remaining, 0)
    end
  end

  defp do_encoded_size(value, depth, remaining) when is_tuple(value) do
    validate_value_depth!(depth)
    count = tuple_size(value)
    validate_collection_size!(count)

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      encoded_tuple_size(value, 0, count, depth + 1, remaining)
    end
  end

  defp do_encoded_size(value, depth, remaining) when is_map(value) do
    validate_value_depth!(depth)
    validate_collection_size!(map_size(value))

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      encoded_map_size(value, depth + 1, remaining)
    end
  end

  defp do_encoded_size(value, _depth, _remaining) do
    raise ArgumentError, "cannot encode native value: #{inspect(value)}"
  end

  defp encoded_list_size([], _depth, remaining, _count), do: {:ok, remaining}

  defp encoded_list_size([_value | _rest], _depth, _remaining, @max_collection_items) do
    raise ArgumentError,
          "native protocol collection exceeds #{@max_collection_items} items"
  end

  defp encoded_list_size([value | rest], depth, remaining, count) do
    case do_encoded_size(value, depth, remaining) do
      {:ok, remaining} -> encoded_list_size(rest, depth, remaining, count + 1)
      :too_large -> :too_large
    end
  end

  defp encoded_tuple_size(_tuple, index, count, _depth, remaining) when index == count,
    do: {:ok, remaining}

  defp encoded_tuple_size(tuple, index, count, depth, remaining) do
    case do_encoded_size(elem(tuple, index), depth, remaining) do
      {:ok, remaining} -> encoded_tuple_size(tuple, index + 1, count, depth, remaining)
      :too_large -> :too_large
    end
  end

  defp encoded_map_size(value, depth, remaining) do
    value
    |> Enum.reduce_while({MapSet.new(), remaining}, fn
      {original_key, item}, {seen, remaining} ->
        key = MapKey.normalize!(original_key)
        ensure_unique_map_key!(seen, key)

        with {:ok, remaining} <- reserve_bytes(remaining, byte_size(key) + 4),
             {:ok, remaining} <- do_encoded_size(item, depth, remaining) do
          {:cont, {MapSet.put(seen, key), remaining}}
        else
          :too_large -> {:halt, :too_large}
        end
    end)
    |> case do
      {_seen, remaining} -> {:ok, remaining}
      :too_large -> :too_large
    end
  end

  defp ensure_unique_map_key!(seen, key) do
    if MapSet.member?(seen, key) do
      raise ArgumentError, "duplicate normalized map key #{inspect(key)}"
    end
  end

  defp reserve_bytes(remaining, size) when size <= remaining,
    do: {:ok, remaining - size}

  defp reserve_bytes(_remaining, _size), do: :too_large

  defp validate_collection_size!(count) when count <= @max_collection_items, do: :ok

  defp validate_collection_size!(_count) do
    raise ArgumentError,
          "native protocol collection exceeds #{@max_collection_items} items"
  end

  defp validate_value_depth!(depth) when depth < @max_value_depth, do: :ok

  defp validate_value_depth!(_depth) do
    raise ArgumentError,
          "native protocol value nesting exceeds #{@max_value_depth} levels"
  end
end
