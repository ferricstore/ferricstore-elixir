defmodule FerricStore.Protocol.CompactBinaryCollections do
  @moduledoc false

  alias FerricStore.BinaryDetacher
  alias FerricStore.Protocol.DecodeBudget
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()

  def take_binary_list(bytes), do: without_budget(take_binary_list(bytes, DecodeBudget.new()))

  def take_binary_list(<<count::32, rest::binary>>, budget)
      when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_binaries(count, rest, [], budget)
    end
  end

  def take_binary_list(<<_count::32, _rest::binary>>, _budget),
    do: {:error, :collection_too_large}

  def take_binary_list(_bytes, _budget), do: {:error, :invalid_compact_binary_list}

  def take_binary_map(bytes), do: without_budget(take_binary_map(bytes, DecodeBudget.new()))

  def take_binary_map(<<count::32, rest::binary>>, budget)
      when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_binary_map_entries(count, rest, %{}, budget)
    end
  end

  def take_binary_map(<<_count::32, _rest::binary>>, _budget),
    do: {:error, :collection_too_large}

  def take_binary_map(_bytes, _budget), do: {:error, :invalid_compact_binary_map}

  def take_binary_list_list(bytes, budget) do
    take_nested(bytes, budget, 0x86, &take_binary_list/2, :invalid_compact_binary_list_list)
  end

  def take_binary_map_list(bytes, budget) do
    take_nested(bytes, budget, 0x87, &take_binary_map/2, :invalid_compact_binary_map_list)
  end

  def take_integer_list(<<0x88, count::32, rest::binary>>, budget)
      when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_integers(count, rest, [], budget)
    end
  end

  def take_integer_list(<<0x88, _count::32, _rest::binary>>, _budget),
    do: {:error, :collection_too_large}

  def take_integer_list(_bytes, _budget), do: {:error, :invalid_compact_integer_list}

  def read_binary(<<0xFFFF_FFFF::32, _rest::binary>>),
    do: {:error, :expected_compact_binary}

  def read_binary(<<size::32, value::binary-size(size), rest::binary>>),
    do: {:ok, BinaryDetacher.detach(value), rest}

  def read_binary(_bytes), do: {:error, :invalid_compact_binary}

  def read_optional_binary(<<0xFFFF_FFFF::32, rest::binary>>), do: {:ok, nil, rest}
  def read_optional_binary(bytes), do: read_binary(bytes)

  defp take_nested(<<tag, count::32, rest::binary>>, budget, tag, decoder, _error)
       when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_values(count, rest, [], budget, decoder)
    end
  end

  defp take_nested(<<tag, _count::32, _rest::binary>>, _budget, tag, _decoder, _error),
    do: {:error, :collection_too_large}

  defp take_nested(_bytes, _budget, _tag, _decoder, error), do: {:error, error}

  defp take_values(0, rest, acc, budget, _decoder),
    do: {:ok, Enum.reverse(acc), rest, budget}

  defp take_values(count, bytes, acc, budget, decoder) when count > 0 do
    with {:ok, value, rest, budget} <- decoder.(bytes, budget) do
      take_values(count - 1, rest, [value | acc], budget, decoder)
    end
  end

  defp take_values(_count, _bytes, _acc, _budget, _decoder),
    do: {:error, :invalid_compact_collection}

  defp take_binaries(0, rest, acc, budget),
    do: {:ok, Enum.reverse(acc), rest, budget}

  defp take_binaries(count, bytes, acc, budget) when count > 0 do
    with {:ok, value, rest} <- read_binary(bytes) do
      take_binaries(count - 1, rest, [value | acc], budget)
    end
  end

  defp take_binaries(_count, _bytes, _acc, _budget),
    do: {:error, :invalid_compact_binaries}

  defp take_binary_map_entries(0, rest, map, budget), do: {:ok, map, rest, budget}

  defp take_binary_map_entries(count, bytes, map, budget) when count > 0 do
    with {:ok, key, rest} <- read_binary(bytes),
         false <- Map.has_key?(map, key),
         {:ok, value, rest} <- read_binary(rest) do
      take_binary_map_entries(count - 1, rest, Map.put(map, key, value), budget)
    else
      true -> {:error, {:duplicate_compact_map_key, %{bytes: duplicate_key_bytes(bytes)}}}
      {:error, _reason} = error -> error
    end
  end

  defp take_binary_map_entries(_count, _bytes, _map, _budget),
    do: {:error, :invalid_compact_binary_map_entries}

  defp take_integers(0, rest, acc, budget),
    do: {:ok, Enum.reverse(acc), rest, budget}

  defp take_integers(count, <<value::signed-64, rest::binary>>, acc, budget) when count > 0,
    do: take_integers(count - 1, rest, [value | acc], budget)

  defp take_integers(_count, _bytes, _acc, _budget),
    do: {:error, :invalid_compact_integers}

  defp without_budget({:ok, value, rest, _budget}), do: {:ok, value, rest}
  defp without_budget({:error, _reason} = error), do: error

  defp duplicate_key_bytes(<<size::32, _rest::binary>>), do: size
  defp duplicate_key_bytes(_bytes), do: 0
end
