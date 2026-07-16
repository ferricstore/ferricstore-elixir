defmodule FerricStore.Protocol.FlowBatchFields do
  @moduledoc false

  alias FerricStore.{BoundedList, RequestLimits}

  @max_collection_items RequestLimits.max_batch_items()
  @min_signed_64 -9_223_372_036_854_775_808
  @max_signed_64 9_223_372_036_854_775_807

  def payload_keys(payload, allowed_keys) do
    if map_size(payload) <= MapSet.size(allowed_keys) and
         Enum.all?(payload, fn {key, _value} -> MapSet.member?(allowed_keys, key) end),
       do: :ok,
       else: :error
  end

  def binary(value) when is_binary(value), do: [<<byte_size(value)::32>>, value]
  def optional_binary(value) when is_binary(value), do: binary(value)

  def optional_binary_value(nil), do: {:ok, nil}
  def optional_binary_value(value) when is_binary(value), do: {:ok, value}
  def optional_binary_value(_value), do: :error

  def bounded_collection_length(value) do
    case BoundedList.count(value, @max_collection_items) do
      {:ok, count} -> {:ok, count}
      {:error, _reason} -> :error
    end
  end

  def collection_length(items, :count_items), do: bounded_collection_length(items)

  def collection_length(_items, count)
      when is_integer(count) and count >= 0 and count <= @max_collection_items,
      do: {:ok, count}

  def collection_length(_items, _count), do: :error

  def flatten({:ok, payload}), do: {:ok, IO.iodata_to_binary(payload)}
  def flatten(:error), do: :error

  def signed_64(value)
      when is_integer(value) and value >= @min_signed_64 and value <= @max_signed_64,
      do: :ok

  def signed_64(_value), do: :error

  def optional_boolean_marker(nil), do: {:ok, 0}
  def optional_boolean_marker(false), do: {:ok, 1}
  def optional_boolean_marker(true), do: {:ok, 2}
  def optional_boolean_marker(_invalid), do: :error

  def return_mode(nil), do: {:ok, 0}
  def return_mode("OK_ON_SUCCESS"), do: {:ok, 1}
  def return_mode("ok_on_success"), do: {:ok, 1}
  def return_mode(_value), do: :error
end
