defmodule FerricStore.Flow.ResponseRecords do
  @moduledoc false

  def decode({:error, _reason} = error, _codec), do: error

  def decode(records, codec) when is_list(records) do
    if Enum.all?(records, &is_map/1),
      do: Enum.map(records, &decode_record(&1, codec)),
      else: records
  end

  def decode(other, _codec), do: other

  @doc false
  def decode_record(record, codec) do
    record
    |> decode_map_field("payload", codec)
    |> decode_map_field("result", codec)
    |> decode_map_field("error", codec)
    |> decode_named_values(codec)
  end

  defp decode_map_field(map, key, codec) do
    case Map.fetch(map, key) do
      {:ok, value} when is_binary(value) -> Map.put(map, key, codec.decode(value))
      _other -> map
    end
  end

  defp decode_named_values(%{"values" => values} = map, codec) when is_map(values) do
    decoded =
      Map.new(values, fn
        {key, value} when is_binary(value) -> {key, codec.decode(value)}
        pair -> pair
      end)

    Map.put(map, "values", decoded)
  end

  defp decode_named_values(map, _codec), do: map
end
