defmodule FerricStore.Protocol.CompactMGetDecoder do
  @moduledoc false

  alias FerricStore.BinaryDetacher
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()

  @spec decode(binary()) :: {:ok, [binary() | nil]} | {:error, term()}
  def decode(<<count::32, rest::binary>>) when count <= @max_collection_items,
    do: decode_values(count, rest, [])

  def decode(<<_count::32, _rest::binary>>), do: {:error, :collection_too_large}
  def decode(_payload), do: {:error, :invalid_compact_mget}

  @spec decode_fixed(binary()) :: {:ok, [binary()]} | {:error, term()}
  def decode_fixed(<<count::32, size::32, rest::binary>>)
      when count <= @max_collection_items and byte_size(rest) == count * size do
    values =
      for offset <- 0..max(count - 1, 0), count > 0 do
        rest |> binary_part(offset * size, size) |> BinaryDetacher.detach()
      end

    {:ok, values}
  end

  def decode_fixed(<<count::32, _size::32, _rest::binary>>)
      when count > @max_collection_items,
      do: {:error, :collection_too_large}

  def decode_fixed(_payload), do: {:error, :invalid_compact_mget_fixed}

  defp decode_values(0, <<>>, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_values(0, _rest, _acc), do: {:error, :trailing_compact_mget_bytes}

  defp decode_values(count, <<0, rest::binary>>, acc),
    do: decode_values(count - 1, rest, [nil | acc])

  defp decode_values(count, <<1, size::32, value::binary-size(size), rest::binary>>, acc),
    do: decode_values(count - 1, rest, [BinaryDetacher.detach(value) | acc])

  defp decode_values(_count, _rest, _acc), do: {:error, :invalid_compact_mget_value}
end
