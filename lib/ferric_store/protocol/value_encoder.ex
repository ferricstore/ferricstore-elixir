defmodule FerricStore.Protocol.ValueEncoder do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  alias FerricStore.Protocol.MapKey
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  def encode(value), do: value |> encode_iodata() |> IO.iodata_to_binary()
  def encode_iodata(value), do: do_encode(value, 0)

  defp do_encode(nil, _depth), do: <<0>>
  defp do_encode(true, _depth), do: <<1>>
  defp do_encode(false, _depth), do: <<2>>

  defp do_encode(value, _depth) when is_signed_64_integer(value),
    do: <<3, value::signed-64>>

  defp do_encode(value, _depth) when is_integer(value) do
    raise ArgumentError,
          "integer #{value} is outside the signed 64-bit native protocol domain"
  end

  defp do_encode(value, _depth) when is_binary(value),
    do: [<<4, byte_size(value)::32>>, value]

  defp do_encode(value, depth) when is_atom(value),
    do: value |> Atom.to_string() |> do_encode(depth)

  defp do_encode(value, _depth) when is_float(value), do: <<7, value::float-64>>

  defp do_encode(value, depth) when is_list(value) do
    validate_value_depth!(depth)
    {count, items} = encode_list(value, depth + 1, 0, [])
    [<<5, count::32>>, items]
  end

  defp do_encode(value, depth) when is_tuple(value) do
    validate_value_depth!(depth)
    count = tuple_size(value)
    validate_collection_size!(count)
    child_depth = depth + 1
    items = value |> Tuple.to_list() |> Enum.map(&do_encode(&1, child_depth))
    [<<5, count::32>>, items]
  end

  defp do_encode(value, depth) when is_map(value) do
    validate_value_depth!(depth)
    count = map_size(value)
    validate_collection_size!(count)
    entries = encode_map_entries(value, depth + 1)
    [<<6, count::32>>, entries]
  end

  defp do_encode(value, _depth) do
    raise ArgumentError, "cannot encode native value: #{inspect(value)}"
  end

  defp encode_map_entries(value, depth) do
    {entries, _seen} =
      Enum.reduce(value, {[], MapSet.new()}, fn {original_key, item}, {entries, seen} ->
        key = MapKey.normalize!(original_key)
        ensure_unique_map_key!(seen, key)
        entry = [<<byte_size(key)::32>>, key, do_encode(item, depth)]
        {[entry | entries], MapSet.put(seen, key)}
      end)

    Enum.reverse(entries)
  end

  defp encode_list([], _depth, count, acc), do: {count, Enum.reverse(acc)}

  defp encode_list([_value | _rest], _depth, @max_collection_items, _acc) do
    raise ArgumentError,
          "native protocol collection exceeds #{@max_collection_items} items"
  end

  defp encode_list([value | rest], depth, count, acc),
    do: encode_list(rest, depth, count + 1, [do_encode(value, depth) | acc])

  defp ensure_unique_map_key!(seen, key) do
    if MapSet.member?(seen, key) do
      raise ArgumentError, "duplicate normalized map key #{inspect(key)}"
    end
  end

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
