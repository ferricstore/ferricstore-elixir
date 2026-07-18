defmodule FerricStore.Flow.Payload.CreateManyMapItem do
  @moduledoc false

  alias FerricStore.Flow.MaxActive
  alias FerricStore.Types

  @allowed_keys ~w(id partition_key payload max_active_ms)

  @spec map(map(), (term() -> {:ok, term()} | {:error, term()})) ::
          {:ok, list() | map()} | {:error, term()}
  def map(item, encode) when map_size(item) <= 4 do
    with {:ok, normalized} <- Types.normalize_map_keys_result(item),
         true <- Enum.all?(Map.keys(normalized), &(&1 in @allowed_keys)),
         id when is_binary(id) and id != "" <- Map.get(normalized, "id"),
         partition_key <- Map.get(normalized, "partition_key"),
         true <- valid_partition?(partition_key),
         {:ok, payload} <- encode_optional(encode, Map.get(normalized, "payload")) do
      format(item, normalized, id, partition_key, payload)
    else
      _invalid -> invalid(item)
    end
  end

  def map(item, _encode), do: invalid(item)

  defp format(item, normalized, id, partition_key, payload) do
    case Map.fetch(normalized, "max_active_ms") do
      {:ok, max_active_ms} ->
        typed(item, normalized, id, partition_key, payload, max_active_ms)

      :error ->
        compact(id, partition_key, payload)
    end
  end

  defp typed(item, normalized, id, partition_key, payload, max_active_ms) do
    if MaxActive.valid?(max_active_ms) do
      mapped =
        %{"id" => id, "max_active_ms" => max_active_ms}
        |> maybe_put("partition_key", partition_key, not is_nil(partition_key))
        |> maybe_put("payload", payload, Map.has_key?(normalized, "payload"))

      {:ok, mapped}
    else
      invalid(item)
    end
  end

  defp compact(id, partition_key, payload) when is_binary(partition_key),
    do: {:ok, [id, partition_key, payload]}

  defp compact(id, nil, payload), do: {:ok, [id, payload]}

  defp valid_partition?(nil), do: true
  defp valid_partition?(value), do: is_binary(value) and value != ""

  defp encode_optional(_encode, nil), do: {:ok, ""}
  defp encode_optional(encode, value), do: encode.(value)

  defp maybe_put(map, key, value, true), do: Map.put(map, key, value)
  defp maybe_put(map, _key, _value, false), do: map

  defp invalid(item), do: {:error, {:invalid_flow_create_many_item, item}}
end
