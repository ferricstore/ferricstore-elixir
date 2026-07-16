defmodule FerricStore.Protocol.PreparedMSet do
  @moduledoc false

  alias FerricStore.Protocol.{PreparedMap, ValueCodec}
  alias FerricStore.RequestLimits

  @max_pairs RequestLimits.max_batch_items()
  @list_header_bytes 5
  @map_header_bytes 5
  @pairs_key "pairs"
  @key_key "key"
  @value_key "value"

  @spec prepare(list(), pos_integer(), [{binary() | atom(), term()}]) ::
          {:ok, PreparedMap.t()} | {:error, :too_large | :invalid_pairs}
  def prepare(pairs, max_bytes, reserved_entries)
      when is_list(pairs) and is_integer(max_bytes) and max_bytes > 0 and
             is_list(reserved_entries) do
    PreparedMap.prepare_encoded(max_bytes, reserved_entries, fn remaining ->
      encode_payload(pairs, remaining)
    end)
  end

  defp encode_payload(pairs, remaining) do
    with {:ok, remaining} <- reserve(remaining, byte_size(@pairs_key) + 4),
         {:ok, remaining} <- reserve(remaining, @list_header_bytes),
         {:ok, count, entries, remaining} <- encode_pairs(pairs, 0, [], remaining) do
      value = [<<5, count::32>>, entries]
      entry = [<<byte_size(@pairs_key)::32>>, @pairs_key, value]
      {:ok, entry, 1, MapSet.new([@pairs_key]), remaining}
    end
  end

  defp encode_pairs([], count, entries, remaining),
    do: {:ok, count, Enum.reverse(entries), remaining}

  defp encode_pairs([_pair | _pairs], @max_pairs, _entries, _remaining), do: :too_large

  defp encode_pairs([pair | pairs], count, entries, remaining) do
    case normalize_pair(pair) do
      {:ok, key, value} ->
        case encode_pair(key, value, remaining) do
          {:ok, encoded, remaining} ->
            encode_pairs(pairs, count + 1, [encoded | entries], remaining)

          :too_large ->
            :too_large
        end

      :error ->
        {:error, :invalid_pairs}
    end
  end

  defp encode_pairs(_improper_tail, _count, _entries, _remaining),
    do: {:error, :invalid_pairs}

  defp encode_pair(key, value, remaining) do
    key_bytes = byte_size(key)

    with {:ok, remaining} <- reserve(remaining, @map_header_bytes),
         {:ok, remaining} <- reserve(remaining, byte_size(@key_key) + 4 + key_bytes + 5),
         {:ok, remaining} <- reserve(remaining, byte_size(@value_key) + 4),
         {:ok, encoded_value, value_bytes} <-
           ValueCodec.encode_iodata_at_depth(value, 3, remaining),
         {:ok, remaining} <- reserve(remaining, value_bytes) do
      encoded_key = [<<4, key_bytes::32>>, key]

      {:ok,
       [
         <<6, 2::32>>,
         <<byte_size(@key_key)::32>>,
         @key_key,
         encoded_key,
         <<byte_size(@value_key)::32>>,
         @value_key,
         encoded_value
       ], remaining}
    else
      {:error, :too_large} -> :too_large
      :too_large -> :too_large
    end
  end

  defp normalize_pair({key, value}) when is_binary(key), do: {:ok, key, value}
  defp normalize_pair([key, value]) when is_binary(key), do: {:ok, key, value}

  defp normalize_pair(%{"key" => key, "value" => value} = pair)
       when is_binary(key) and map_size(pair) == 2,
       do: {:ok, key, value}

  defp normalize_pair(%{key: key, value: value} = pair)
       when is_binary(key) and map_size(pair) == 2,
       do: {:ok, key, value}

  defp normalize_pair(_pair), do: :error

  defp reserve(remaining, bytes) when bytes <= remaining, do: {:ok, remaining - bytes}
  defp reserve(_remaining, _bytes), do: :too_large
end
