defmodule FerricStore.Flow.QueryResponse.Indexes do
  @moduledoc false

  alias FerricStore.Flow.{QueryIndex, QueryIndexFormat, QueryIndexStatus}
  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Types

  @contract "ferric.flow.query.indexes/v1"

  def decode(value) when is_map(value) do
    with {:ok, @contract} <- V.contract(value, "contract_version", @contract),
         {:ok, observed} <- V.non_negative(value, "observed_at_ms"),
         {:ok, max_age} <- V.non_negative(value, "statistics_max_age_ms"),
         {:ok, registry} <- registry(Types.get(value, "registry")),
         {:ok, services} <- V.required_map(value, "services"),
         {:ok, indexes} <- entries(Types.get(value, "indexes")) do
      {:ok,
       %QueryIndexStatus{
         contract_version: @contract,
         observed_at_ms: observed,
         statistics_max_age_ms: max_age,
         registry: registry,
         services: services,
         indexes: indexes,
         raw: value
       }}
    end
  end

  def decode(value), do: V.invalid(:indexes, value)

  defp registry(value) when is_map(value) do
    with {:ok, epoch} <- V.unsigned(value, "epoch"),
         {:ok, catalog_version} <- V.positive_unsigned(value, "catalog_version"),
         do: {:ok, %{epoch: epoch, catalog_version: catalog_version}}
  end

  defp registry(value), do: V.invalid(:registry, value)

  defp entries(entries) when is_list(entries) and length(entries) <= 32 do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case entry(entry) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, V.invalid({:index, index}, reason)}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp entries(value), do: V.invalid(:indexes, value)

  defp entry(value) when is_map(value) do
    with {:ok, id} <- V.required_binary(value, "id"),
         {:ok, version} <- V.positive_unsigned(value, "version"),
         {:ok, build_id} <- V.required_binary(value, "build_id"),
         {:ok, state} <- V.required_binary(value, "state"),
         {:ok, queryable} <- V.required_boolean(value, "queryable"),
         {:ok, covering_fields} <- covering_fields(Types.get(value, "covering_fields")),
         {:ok, format} <- format(Types.get(value, "format")) do
      {:ok,
       %QueryIndex{
         id: id,
         version: version,
         build_id: build_id,
         state: state,
         queryable: queryable,
         covering_fields: covering_fields,
         format: format,
         raw: value
       }}
    end
  end

  defp entry(value), do: V.invalid(:index, value)

  defp covering_fields(fields) when is_list(fields) and length(fields) <= 32 do
    fields
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, {[], MapSet.new()}}, fn {field, index}, {:ok, {acc, seen}} ->
      add_covering_field(field, index, fields, acc, seen)
    end)
    |> then(fn
      {:ok, {decoded, _seen}} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end)
  end

  defp covering_fields(value), do: V.invalid(:covering_fields, value)

  defp add_covering_field(field, index, fields, acc, seen) do
    case bounded_text(field, 512) do
      {:ok, decoded} -> add_unique_covering_field(decoded, fields, acc, seen)
      :error -> {:halt, V.invalid({:covering_fields, index}, field)}
    end
  end

  defp add_unique_covering_field(field, fields, acc, seen) do
    if MapSet.member?(seen, field) do
      {:halt, V.invalid(:covering_fields, fields)}
    else
      {:cont, {:ok, {[field | acc], MapSet.put(seen, field)}}}
    end
  end

  defp format(value) when is_map(value) do
    with {:ok, query_row} <- V.bounded_binary(value, "query_row", 128),
         {:ok, key} <- V.bounded_binary(value, "key", 128),
         {:ok, entry} <- V.bounded_binary(value, "entry", 128),
         {:ok, reverse} <- V.bounded_binary(value, "reverse", 128),
         {:ok, counter} <- nullable_counter(value) do
      {:ok,
       %QueryIndexFormat{
         query_row: query_row,
         key: key,
         entry: entry,
         reverse: reverse,
         counter: counter,
         raw: value
       }}
    end
  end

  defp format(value), do: V.invalid(:format, value)

  defp nullable_counter(value) do
    if V.has_key?(value, "counter") do
      case Types.get(value, "counter") do
        nil -> {:ok, nil}
        _present -> V.bounded_binary(value, "counter", 128)
      end
    else
      V.invalid({:format, :counter}, nil)
    end
  end

  defp bounded_text(value, maximum_bytes)
       when is_binary(value) and value != "" and byte_size(value) <= maximum_bytes do
    if String.valid?(value), do: {:ok, value}, else: :error
  end

  defp bounded_text(_value, _maximum_bytes), do: :error
end
