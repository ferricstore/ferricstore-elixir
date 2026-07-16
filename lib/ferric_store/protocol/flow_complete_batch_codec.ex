defmodule FerricStore.Protocol.FlowCompleteBatchCodec do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.RequestLimits

  @max_items RequestLimits.max_batch_items()
  @min_signed_64 -9_223_372_036_854_775_808
  @max_signed_64 9_223_372_036_854_775_807
  @keys MapSet.new(["items", "now_ms", "partition_key", "independent", "return"])

  @spec payload(map()) :: {:ok, binary()} | :error
  def payload(payload) do
    payload
    |> iodata_payload()
    |> flatten()
  end

  @spec iodata_payload(map()) :: {:ok, iodata()} | :error
  def iodata_payload(payload), do: iodata_payload(payload, :count_items)

  @spec iodata_payload(map(), non_neg_integer() | :count_items) :: {:ok, iodata()} | :error
  def iodata_payload(%{"now_ms" => now_ms, "items" => items} = payload, item_count)
      when is_integer(now_ms) and is_list(items) do
    with :ok <- payload_keys(payload),
         :ok <- signed_64(now_ms),
         {:ok, independent} <- boolean_marker(Map.get(payload, "independent")),
         {:ok, partition_key} <- optional_binary_value(Map.get(payload, "partition_key")),
         {:ok, item_count} <- collection_length(items, item_count),
         {:ok, item_bytes, ^item_count} <- claimed_items(items),
         {:ok, tag} <- return_tag(Map.get(payload, "return")) do
      {:ok,
       [
         <<tag>>,
         optional_binary(partition_key),
         <<now_ms::signed-64, independent::8, item_count::32>>,
         item_bytes
       ]}
    else
      _error -> :error
    end
  end

  def iodata_payload(_payload, _item_count), do: :error

  defp claimed_items(items) do
    items
    |> Enum.reduce_while({[], 0}, fn item, {encoded, count} ->
      case claimed_item(item) do
        {:ok, bytes} -> {:cont, {[bytes | encoded], count + 1}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      {encoded, count} -> {:ok, Enum.reverse(encoded), count}
    end
  end

  defp claimed_item([id, lease_token, fencing_token])
       when is_binary(id) and is_binary(lease_token) and is_integer(fencing_token) and
              fencing_token >= @min_signed_64 and fencing_token <= @max_signed_64 do
    {:ok, [binary(id), optional_binary(nil), binary(lease_token), <<fencing_token::signed-64>>]}
  end

  defp claimed_item([id, partition_key, lease_token, fencing_token])
       when is_binary(id) and is_binary(partition_key) and is_binary(lease_token) and
              is_integer(fencing_token) and fencing_token >= @min_signed_64 and
              fencing_token <= @max_signed_64 do
    {:ok,
     [
       binary(id),
       optional_binary(partition_key),
       binary(lease_token),
       <<fencing_token::signed-64>>
     ]}
  end

  defp claimed_item(_item), do: :error

  defp payload_keys(payload) do
    if map_size(payload) <= MapSet.size(@keys) and
         payload |> Map.keys() |> Enum.all?(&MapSet.member?(@keys, &1)),
       do: :ok,
       else: :error
  end

  defp collection_length(items, :count_items) do
    case BoundedList.count(items, @max_items) do
      {:ok, count} -> {:ok, count}
      {:error, _reason} -> :error
    end
  end

  defp collection_length(_items, count)
       when is_integer(count) and count >= 0 and count <= @max_items,
       do: {:ok, count}

  defp collection_length(_items, _count), do: :error

  defp binary(value), do: [<<byte_size(value)::32>>, value]
  defp optional_binary(nil), do: <<0xFFFF_FFFF::32>>
  defp optional_binary(value), do: binary(value)

  defp optional_binary_value(nil), do: {:ok, nil}
  defp optional_binary_value(value) when is_binary(value), do: {:ok, value}
  defp optional_binary_value(_value), do: :error

  defp signed_64(value) when value >= @min_signed_64 and value <= @max_signed_64, do: :ok
  defp signed_64(_value), do: :error

  defp boolean_marker(nil), do: {:ok, 0}
  defp boolean_marker(false), do: {:ok, 1}
  defp boolean_marker(true), do: {:ok, 2}
  defp boolean_marker(_value), do: :error

  defp return_tag(nil), do: {:ok, 0x92}
  defp return_tag(value) when value in ["OK_ON_SUCCESS", "ok_on_success"], do: {:ok, 0x93}
  defp return_tag(_value), do: :error

  defp flatten({:ok, payload}), do: {:ok, IO.iodata_to_binary(payload)}
  defp flatten(:error), do: :error
end
