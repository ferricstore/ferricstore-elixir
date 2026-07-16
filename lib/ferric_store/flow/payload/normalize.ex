defmodule FerricStore.Flow.Payload.Normalize do
  @moduledoc false

  alias FerricStore.Flow.CodecRuntime
  alias FerricStore.Flow.Options.{PreparedMap, PreparedValue, PreparedValues}
  alias FerricStore.Types

  def put_if_present(map, _key, nil), do: map
  def put_if_present(map, key, value), do: Map.put(map, key, value)

  def encoded_or_nil(_codec, nil), do: nil
  def encoded_or_nil(_codec, %PreparedValue{value: value}), do: value
  def encoded_or_nil(codec, value), do: CodecRuntime.encode(codec, value)

  def stringify_map(nil), do: nil
  def stringify_map(%PreparedMap{value: map}), do: map
  def stringify_map(map) when is_map(map), do: Types.normalize_map(map)

  def stringify_nested_map(nil), do: nil
  def stringify_nested_map(%PreparedMap{value: map}), do: map

  def stringify_nested_map(value) when is_map(value) or is_list(value),
    do: Types.normalize_map(value)

  def stringify_nested_map(value), do: value

  def encode_value_map(_codec, nil), do: nil
  def encode_value_map(_codec, %PreparedValues{value: map}), do: map

  def encode_value_map(codec, %PreparedMap{value: map}) do
    Map.new(map, fn {key, value} -> {key, CodecRuntime.encode(codec, value)} end)
  end

  def encode_value_map(codec, map) when is_map(map) do
    map
    |> Types.normalize_map_keys()
    |> Map.new(fn {key, value} -> {key, CodecRuntime.encode(codec, value)} end)
  end

  def map_value(map, string_key, atom_key) do
    case Map.fetch(map, string_key) do
      {:ok, value} -> value
      :error -> Map.get(map, atom_key)
    end
  end

  def now_ms, do: System.system_time(:millisecond)
end
