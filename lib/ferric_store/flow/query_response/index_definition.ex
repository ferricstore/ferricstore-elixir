defmodule FerricStore.Flow.QueryResponse.IndexDefinition do
  @moduledoc false

  alias FerricStore.Flow.{QueryIndex, QueryIndexField, QueryIndexFormat}

  alias FerricStore.Flow.QueryResponse.{
    IndexLifecycle,
    IndexStatistics,
    Validation
  }

  alias FerricStore.Types

  def decode(value) when is_map(value) do
    with {:ok, id} <- Validation.bounded_binary(value, "id", 64),
         {:ok, version} <- Validation.positive_unsigned(value, "version"),
         {:ok, build_id} <- Validation.bounded_binary(value, "build_id", 128),
         {:ok, source} <- choice(value, "source", ["runs"], :source),
         {:ok, state} <-
           choice(value, "state", ~w(building validating active retiring failed), :state),
         {:ok, queryable} <- Validation.required_boolean(value, "queryable"),
         {:ok, fields} <- fields(Types.get(value, "fields")),
         {:ok, workloads} <- unique_texts(Types.get(value, "workloads"), 16, 64, :workloads),
         {:ok, count_prefixes} <-
           count_prefixes(Types.get(value, "count_prefixes"), length(fields)),
         {:ok, covering_fields} <-
           unique_texts(Types.get(value, "covering_fields"), 32, 512, :covering_fields),
         {:ok, format} <- format(Types.get(value, "format")),
         {:ok, coverage} <- IndexLifecycle.coverage(Types.get(value, "coverage")),
         {:ok, build} <- IndexLifecycle.build(Types.get(value, "build")),
         {:ok, validation} <- IndexLifecycle.validation(Types.get(value, "validation")),
         {:ok, retirement} <- IndexLifecycle.retirement(Types.get(value, "retirement")),
         {:ok, statistics} <- IndexStatistics.decode(Types.get(value, "statistics")) do
      {:ok,
       %QueryIndex{
         id: id,
         version: version,
         build_id: build_id,
         source: source,
         state: state,
         queryable: queryable,
         fields: fields,
         workloads: workloads,
         count_prefixes: count_prefixes,
         covering_fields: covering_fields,
         format: format,
         coverage: coverage,
         build: build,
         validation: validation,
         retirement: retirement,
         statistics: statistics,
         raw: value
       }}
    end
  end

  def decode(value), do: Validation.invalid(:index, value)

  defp fields(value) when is_list(value) and length(value) in 2..8 do
    value
    |> Enum.reduce_while({:ok, {[], MapSet.new()}}, fn field, {:ok, {acc, seen}} ->
      with true <- is_map(field),
           {:ok, name} <- Validation.bounded_binary(field, "name", 512),
           false <- MapSet.member?(seen, name),
           {:ok, direction} <- choice(field, "direction", ~w(asc desc), :field_direction),
           {:ok, encoding} <- choice(field, "encoding", ~w(hashed ordered), :field_encoding) do
        decoded = %QueryIndexField{
          name: name,
          direction: direction,
          encoding: encoding,
          raw: field
        }

        {:cont, {:ok, {[decoded | acc], MapSet.put(seen, name)}}}
      else
        _invalid -> {:halt, Validation.invalid(:fields, value)}
      end
    end)
    |> then(fn
      {:ok, {decoded, _seen}} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end)
  end

  defp fields(value), do: Validation.invalid(:fields, value)

  defp count_prefixes(value, field_count) when is_list(value) and length(value) <= field_count do
    if Enum.all?(value, &(is_integer(&1) and &1 > 0 and &1 <= field_count)) and
         value == Enum.sort(Enum.uniq(value)),
       do: {:ok, value},
       else: Validation.invalid(:count_prefixes, value)
  end

  defp count_prefixes(value, _field_count), do: Validation.invalid(:count_prefixes, value)

  defp format(value) when is_map(value) do
    with {:ok, query_row} <- Validation.bounded_binary(value, "query_row", 128),
         {:ok, key} <- Validation.bounded_binary(value, "key", 128),
         {:ok, entry} <- Validation.bounded_binary(value, "entry", 128),
         {:ok, reverse} <- Validation.bounded_binary(value, "reverse", 128),
         {:ok, counter} <- nullable_text(value, "counter", 128) do
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

  defp format(value), do: Validation.invalid(:format, value)

  defp unique_texts(value, maximum, maximum_bytes, field)
       when is_list(value) and length(value) <= maximum do
    with true <-
           Enum.all?(value, fn text ->
             is_binary(text) and text != "" and byte_size(text) <= maximum_bytes and
               String.valid?(text)
           end),
         true <- length(value) == MapSet.size(MapSet.new(value)) do
      {:ok, value}
    else
      false -> Validation.invalid(field, value)
    end
  end

  defp unique_texts(value, _maximum, _maximum_bytes, field),
    do: Validation.invalid(field, value)

  defp nullable_text(value, field, maximum_bytes) do
    if Validation.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> Validation.bounded_binary(value, field, maximum_bytes)
      end
    else
      Validation.invalid({:nullable, field}, value)
    end
  end

  defp choice(value, field, choices, error_field) do
    actual = Types.get(value, field)
    if actual in choices, do: {:ok, actual}, else: Validation.invalid(error_field, actual)
  end
end
