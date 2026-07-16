defmodule FerricStore.Protocol.BoundedValueEncoder do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  alias FerricStore.Protocol.MapKey
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  def encode_iodata(value, max_bytes) when is_integer(max_bytes) and max_bytes >= 0,
    do: encode_at_depth(value, 0, max_bytes)

  def encode_iodata_at_depth(value, depth, max_bytes)
      when is_integer(depth) and depth >= 0 and is_integer(max_bytes) and max_bytes >= 0 do
    encode_at_depth(value, depth, max_bytes)
  end

  defp encode_at_depth(value, depth, max_bytes) do
    case do_encode(value, depth, max_bytes) do
      {:ok, encoded, remaining} -> {:ok, encoded, max_bytes - remaining}
      :too_large -> {:error, :too_large}
    end
  end

  defp do_encode(nil, _depth, remaining), do: bounded_scalar(<<0>>, remaining)
  defp do_encode(true, _depth, remaining), do: bounded_scalar(<<1>>, remaining)
  defp do_encode(false, _depth, remaining), do: bounded_scalar(<<2>>, remaining)

  defp do_encode(value, _depth, remaining) when is_signed_64_integer(value),
    do: bounded_scalar(<<3, value::signed-64>>, remaining)

  defp do_encode(value, _depth, _remaining) when is_integer(value) do
    raise ArgumentError,
          "integer #{value} is outside the signed 64-bit native protocol domain"
  end

  defp do_encode(value, _depth, remaining) when is_binary(value) do
    size = byte_size(value)

    case reserve_bytes(remaining, size + 5) do
      {:ok, remaining} -> {:ok, [<<4, size::32>>, value], remaining}
      :too_large -> :too_large
    end
  end

  defp do_encode(value, depth, remaining) when is_atom(value),
    do: value |> Atom.to_string() |> do_encode(depth, remaining)

  defp do_encode(value, _depth, remaining) when is_float(value),
    do: bounded_scalar(<<7, value::float-64>>, remaining)

  defp do_encode(value, depth, remaining) when is_list(value) do
    validate_value_depth!(depth)

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      case encode_list(value, depth + 1, remaining, 0, []) do
        {:ok, count, items, remaining} -> {:ok, [<<5, count::32>>, items], remaining}
        :too_large -> :too_large
      end
    end
  end

  defp do_encode(value, depth, remaining) when is_tuple(value) do
    validate_value_depth!(depth)
    count = tuple_size(value)
    validate_collection_size!(count)

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      case encode_tuple(value, 0, count, depth + 1, remaining, []) do
        {:ok, items, remaining} -> {:ok, [<<5, count::32>>, items], remaining}
        :too_large -> :too_large
      end
    end
  end

  defp do_encode(value, depth, remaining) when is_map(value) do
    validate_value_depth!(depth)
    count = map_size(value)
    validate_collection_size!(count)

    with {:ok, remaining} <- reserve_bytes(remaining, 5) do
      case encode_map(value, depth + 1, remaining) do
        {:ok, entries, remaining} -> {:ok, [<<6, count::32>>, entries], remaining}
        :too_large -> :too_large
      end
    end
  end

  defp do_encode(value, _depth, _remaining) do
    raise ArgumentError, "cannot encode native value: #{inspect(value)}"
  end

  defp encode_list([], _depth, remaining, count, acc),
    do: {:ok, count, Enum.reverse(acc), remaining}

  defp encode_list([_value | _rest], _depth, _remaining, @max_collection_items, _acc) do
    raise ArgumentError,
          "native protocol collection exceeds #{@max_collection_items} items"
  end

  defp encode_list([value | rest], depth, remaining, count, acc) do
    case do_encode(value, depth, remaining) do
      {:ok, encoded, remaining} ->
        encode_list(rest, depth, remaining, count + 1, [encoded | acc])

      :too_large ->
        :too_large
    end
  end

  defp encode_tuple(_tuple, index, count, _depth, remaining, acc) when index == count,
    do: {:ok, Enum.reverse(acc), remaining}

  defp encode_tuple(tuple, index, count, depth, remaining, acc) do
    case do_encode(elem(tuple, index), depth, remaining) do
      {:ok, encoded, remaining} ->
        encode_tuple(tuple, index + 1, count, depth, remaining, [encoded | acc])

      :too_large ->
        :too_large
    end
  end

  defp encode_map(value, depth, remaining) do
    value
    |> Enum.reduce_while({[], MapSet.new(), remaining}, fn
      {original_key, item}, {entries, seen, remaining} ->
        case encode_map_entry(original_key, item, seen, depth, remaining) do
          {:ok, entry, key, remaining} ->
            {:cont, {[entry | entries], MapSet.put(seen, key), remaining}}

          :too_large ->
            {:halt, :too_large}
        end
    end)
    |> case do
      {entries, _seen, remaining} -> {:ok, Enum.reverse(entries), remaining}
      :too_large -> :too_large
    end
  end

  defp encode_map_entry(original_key, item, seen, depth, remaining) do
    key = MapKey.normalize!(original_key)
    ensure_unique_map_key!(seen, key)

    with {:ok, remaining} <- reserve_bytes(remaining, byte_size(key) + 4),
         {:ok, encoded, remaining} <- do_encode(item, depth, remaining) do
      {:ok, [<<byte_size(key)::32>>, key, encoded], key, remaining}
    else
      :too_large -> :too_large
    end
  end

  defp ensure_unique_map_key!(seen, key) do
    if MapSet.member?(seen, key),
      do: raise(ArgumentError, "duplicate normalized map key #{inspect(key)}")
  end

  defp bounded_scalar(encoded, remaining) do
    case reserve_bytes(remaining, byte_size(encoded)) do
      {:ok, remaining} -> {:ok, encoded, remaining}
      :too_large -> :too_large
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
