defmodule FerricStore.Protocol.ValueCodec do
  @moduledoc false

  alias FerricStore.BinaryDetacher
  alias FerricStore.Protocol.{BoundedValueEncoder, ValueEncoder, ValueSizer}
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  @spec encode(term()) :: binary()
  defdelegate encode(value), to: ValueEncoder

  @doc false
  @spec encode_iodata(term()) :: iodata()
  defdelegate encode_iodata(value), to: ValueEncoder

  @doc false
  @spec encode_iodata(term(), non_neg_integer()) ::
          {:ok, iodata(), non_neg_integer()} | {:error, :too_large}
  defdelegate encode_iodata(value, max_bytes), to: BoundedValueEncoder

  @doc false
  @spec encode_iodata_at_depth(term(), non_neg_integer(), non_neg_integer()) ::
          {:ok, iodata(), non_neg_integer()} | {:error, :too_large}
  defdelegate encode_iodata_at_depth(value, depth, max_bytes), to: BoundedValueEncoder

  @doc false
  @spec encoded_size(term(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, :too_large}
  defdelegate encoded_size(value, max_bytes), to: ValueSizer

  @spec decode(binary()) :: {:ok, term(), binary()} | {:error, term()}
  def decode(bytes) when is_binary(bytes) do
    case decode_with_budget(bytes, @max_collection_items) do
      {:ok, value, rest, _remaining_items} -> {:ok, value, rest}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec decode_with_budget(binary(), non_neg_integer()) ::
          {:ok, term(), binary(), non_neg_integer()} | {:error, term()}
  def decode_with_budget(bytes, remaining_items)
      when is_binary(bytes) and is_integer(remaining_items) and remaining_items >= 0,
      do: do_decode(bytes, 0, remaining_items)

  defp do_decode(<<0, rest::binary>>, _depth, remaining), do: {:ok, nil, rest, remaining}
  defp do_decode(<<1, rest::binary>>, _depth, remaining), do: {:ok, true, rest, remaining}
  defp do_decode(<<2, rest::binary>>, _depth, remaining), do: {:ok, false, rest, remaining}

  defp do_decode(<<3, value::signed-64, rest::binary>>, _depth, remaining),
    do: {:ok, value, rest, remaining}

  defp do_decode(
         <<4, length::32, bytes::binary-size(length), rest::binary>>,
         _depth,
         remaining
       ),
       do: {:ok, BinaryDetacher.detach(bytes), rest, remaining}

  defp do_decode(<<7, value::float-64, rest::binary>>, _depth, remaining),
    do: {:ok, value, rest, remaining}

  defp do_decode(<<8, value::unsigned-64, rest::binary>>, _depth, remaining),
    do: {:ok, value, rest, remaining}

  defp do_decode(<<type, count::32, _rest::binary>>, _depth, remaining)
       when type in [5, 6] and
              (count > @max_collection_items or count > remaining),
       do: {:error, :collection_too_large}

  defp do_decode(<<5, count::32, rest::binary>>, depth, remaining)
       when depth < @max_value_depth,
       do: decode_list(count, rest, [], depth + 1, remaining - count)

  defp do_decode(<<6, count::32, rest::binary>>, depth, remaining)
       when depth < @max_value_depth,
       do: decode_map(count, rest, %{}, depth + 1, remaining - count)

  defp do_decode(<<type, _count::32, _rest::binary>>, _depth, _remaining)
       when type in [5, 6],
       do: {:error, :value_nesting_too_deep}

  defp do_decode(_bytes, _depth, _remaining), do: {:error, :invalid_value}

  defp decode_list(0, rest, acc, _depth, remaining),
    do: {:ok, Enum.reverse(acc), rest, remaining}

  defp decode_list(count, bytes, acc, depth, remaining) do
    with {:ok, value, rest, remaining} <- do_decode(bytes, depth, remaining) do
      decode_list(count - 1, rest, [value | acc], depth, remaining)
    end
  end

  defp decode_map(0, rest, acc, _depth, remaining), do: {:ok, acc, rest, remaining}

  defp decode_map(
         count,
         <<key_length::32, key::binary-size(key_length), rest::binary>>,
         acc,
         depth,
         remaining
       ) do
    key = BinaryDetacher.detach(key)

    if Map.has_key?(acc, key) do
      {:error, {:duplicate_map_key, %{bytes: byte_size(key)}}}
    else
      with {:ok, value, rest, remaining} <- do_decode(rest, depth, remaining) do
        decode_map(count - 1, rest, Map.put(acc, key, value), depth, remaining)
      end
    end
  end

  defp decode_map(_count, _bytes, _acc, _depth, _remaining), do: {:error, :invalid_map}
end
