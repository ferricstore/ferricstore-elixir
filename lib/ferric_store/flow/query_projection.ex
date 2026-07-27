defmodule FerricStore.Flow.QueryProjection do
  @moduledoc """
  Builds bounded, source-aware sparse return projections for FQL1 queries.

  The input query must start with `FROM runs` or `FROM events` and must not
  already contain a `RETURN` clause.
  """

  @max_query_bytes 16 * 1_024
  @max_fields 32
  @max_dynamic_name_bytes 64

  alias FerricStore.Flow.QueryText

  @run_fields MapSet.new([
                :run_id,
                :type,
                :state,
                :version,
                :priority,
                :partition_key,
                :created_at_ms,
                :updated_at_ms,
                :next_run_at_ms,
                :lease_deadline_ms,
                :attempts,
                :run_state,
                :max_active_ms,
                :parent_flow_id,
                :root_flow_id,
                :correlation_id,
                :attributes,
                :state_meta
              ])
  @event_fields MapSet.new([:event_id, :fields])

  @type shape :: :record | :records
  @type field ::
          atom()
          | {:attribute, binary()}
          | {:state_meta, binary(), binary()}
          | {:event_field, binary()}

  @spec project(binary(), shape(), [field()]) ::
          {:ok, binary()} | {:error, {:invalid_flow_query_projection, atom()}}
  def project(query, shape, fields) do
    with :ok <- validate_query(query),
         :ok <- validate_shape(shape),
         :ok <- validate_field_count(fields),
         {:ok, source} <- query_source(query),
         {:ok, selectors} <- selectors(source, fields),
         false <- return_clause?(query),
         :ok <- validate_terminator(query),
         projected <- append_projection(query, shape, selectors),
         :ok <- validate_query(projected) do
      {:ok, projected}
    else
      true -> error(:return_clause_present)
      {:error, {:invalid_flow_query_projection, _reason}} = error -> error
    end
  end

  defp validate_query(query)
       when is_binary(query) and query != "" and byte_size(query) <= @max_query_bytes do
    if String.valid?(query) and String.trim(query) != "", do: :ok, else: error(:invalid_query)
  end

  defp validate_query(_query), do: error(:invalid_query)

  defp validate_shape(shape) when shape in [:record, :records], do: :ok
  defp validate_shape(_shape), do: error(:invalid_shape)

  defp validate_field_count(fields)
       when is_list(fields) and length(fields) in 1..@max_fields,
       do: :ok

  defp validate_field_count(_fields), do: error(:field_limit)

  defp query_source(query) do
    query = QueryText.trim_leading(query)

    with {:ok, rest} <- QueryText.after_ascii_keyword(query, "FROM"),
         rest <- QueryText.trim_leading(rest),
         true <- rest != "",
         {:ok, source} <- projection_source(rest) do
      {:ok, source}
    else
      _invalid -> error(:invalid_source)
    end
  end

  defp projection_source(query) do
    cond do
      match?({:ok, _rest}, QueryText.after_ascii_keyword(query, "RUNS")) -> {:ok, :runs}
      match?({:ok, _rest}, QueryText.after_ascii_keyword(query, "EVENTS")) -> {:ok, :events}
      true -> :error
    end
  end

  defp selectors(source, fields) do
    fields
    |> Enum.reduce_while({:ok, [], MapSet.new()}, &reduce_selector(source, &1, &2))
    |> case do
      {:ok, selectors, _seen} -> {:ok, Enum.reverse(selectors)}
      {:error, _reason} = error -> error
    end
  end

  defp reduce_selector(source, field, {:ok, selectors, seen}) do
    case selector(source, field) do
      {:ok, selector} -> add_unique_selector(selector, selectors, seen)
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp add_unique_selector(selector, selectors, seen) do
    if MapSet.member?(seen, selector),
      do: {:halt, error(:duplicate_field)},
      else: {:cont, {:ok, [selector | selectors], MapSet.put(seen, selector)}}
  end

  defp selector(:runs, field) when is_atom(field) do
    if MapSet.member?(@run_fields, field),
      do: {:ok, Atom.to_string(field)},
      else: error(:field_source)
  end

  defp selector(:runs, {:attribute, name}) do
    with {:ok, quoted} <- quote_name(name, false), do: {:ok, "attribute[" <> quoted <> "]"}
  end

  defp selector(:runs, {:state_meta, state, name}) do
    with {:ok, quoted_state} <- quote_name(state, true),
         {:ok, quoted_name} <- quote_name(name, false) do
      {:ok, "state_meta[" <> quoted_state <> "][" <> quoted_name <> "]"}
    end
  end

  defp selector(:events, field) when is_atom(field) do
    if MapSet.member?(@event_fields, field),
      do: {:ok, Atom.to_string(field)},
      else: error(:field_source)
  end

  defp selector(:events, {:event_field, name}) do
    with {:ok, quoted} <- quote_name(name, false), do: {:ok, "fields[" <> quoted <> "]"}
  end

  defp selector(_source, _field), do: error(:field_source)

  defp quote_name(value, allow_private)
       when is_binary(value) and value != "" and byte_size(value) <= @max_dynamic_name_bytes do
    if String.valid?(value) and (allow_private or not String.starts_with?(value, "__")) do
      {:ok, "'" <> String.replace(value, "'", "''") <> "'"}
    else
      error(:invalid_dynamic_name)
    end
  end

  defp quote_name(_value, _allow_private), do: error(:invalid_dynamic_name)

  defp append_projection(query, shape, selectors) do
    query =
      query
      |> QueryText.trim()
      |> trim_one_terminator()
      |> QueryText.trim_trailing()

    query <>
      " RETURN " <>
      (shape |> Atom.to_string() |> String.upcase()) <>
      " (" <> Enum.join(selectors, ", ") <> ")"
  end

  defp validate_terminator(query) do
    trimmed = QueryText.trim(query)

    if String.ends_with?(trimmed, ";") do
      base = trimmed |> trim_one_terminator() |> QueryText.trim_trailing()

      if String.ends_with?(base, ";"), do: error(:invalid_query), else: :ok
    else
      :ok
    end
  end

  defp trim_one_terminator(query) do
    size = byte_size(query)

    if size > 0 and :binary.last(query) == ?;,
      do: binary_part(query, 0, size - 1),
      else: query
  end

  defp return_clause?(query) do
    query
    |> strip_quoted(false, [])
    |> IO.iodata_to_binary()
    |> then(&Regex.match?(~r/(?:^|[^A-Za-z0-9_])RETURN(?:$|[^A-Za-z0-9_])/iu, &1))
  end

  defp strip_quoted(<<>>, _quoted, acc), do: Enum.reverse(acc)

  defp strip_quoted(<<"''", rest::binary>>, true, acc),
    do: strip_quoted(rest, true, ["  " | acc])

  defp strip_quoted(<<"'", rest::binary>>, quoted, acc),
    do: strip_quoted(rest, not quoted, [" " | acc])

  defp strip_quoted(<<_byte, rest::binary>>, true, acc),
    do: strip_quoted(rest, true, [" " | acc])

  defp strip_quoted(<<byte, rest::binary>>, false, acc),
    do: strip_quoted(rest, false, [<<byte>> | acc])

  defp error(reason), do: {:error, {:invalid_flow_query_projection, reason}}
end
