defmodule FerricStore.Flow.QueryResponse.Validation do
  @moduledoc false

  alias FerricStore.Types

  @maximum_signed_64 9_223_372_036_854_775_807
  @maximum_unsigned_64 18_446_744_073_709_551_615

  @usage_fields [
    {"range_seeks", :range_seeks},
    {"range_pages", :range_pages},
    {"scanned_entries", :scanned_entries},
    {"scanned_bytes", :scanned_bytes},
    {"hydrated_records", :hydrated_records},
    {"residual_checks", :residual_checks},
    {"duplicate_entries", :duplicate_entries},
    {"result_records", :result_records},
    {"response_bytes", :response_bytes},
    {"memory_high_water_bytes", :memory_high_water_bytes},
    {"wall_time_us", :wall_time_us}
  ]

  def contract(value, field, expected) do
    case Types.get(value, field) do
      ^expected -> {:ok, expected}
      actual -> invalid({:contract, field}, actual)
    end
  end

  def required_binary(value, field) do
    case Types.get(value, field) do
      binary when is_binary(binary) and binary != "" ->
        if String.valid?(binary), do: {:ok, binary}, else: invalid({:binary, field}, binary)

      actual ->
        invalid({:binary, field}, actual)
    end
  end

  def optional_binary(value, field) do
    case Types.get(value, field) do
      nil ->
        {:ok, nil}

      binary when is_binary(binary) ->
        if String.valid?(binary), do: {:ok, binary}, else: invalid({:binary, field}, binary)

      actual ->
        invalid({:binary, field}, actual)
    end
  end

  def bounded_binary(value, field, maximum_bytes) do
    with {:ok, binary} <- required_binary(value, field),
         true <- byte_size(binary) <= maximum_bytes do
      {:ok, binary}
    else
      {:error, _reason} = error -> error
      false -> invalid({:binary, field}, Types.get(value, field))
    end
  end

  def query_fingerprint(value, field) do
    with {:ok, fingerprint} <- required_binary(value, field),
         {:ok, decoded} <- Base.decode16(fingerprint, case: :mixed),
         true <- byte_size(decoded) == 32 do
      {:ok, fingerprint}
    else
      _invalid -> invalid(:query_fingerprint, Types.get(value, field))
    end
  end

  def required_boolean(value, field) do
    case Types.get(value, field) do
      boolean when is_boolean(boolean) -> {:ok, boolean}
      actual -> invalid({:boolean, field}, actual)
    end
  end

  def required_map(value, field) do
    case Types.get(value, field) do
      map when is_map(map) -> {:ok, map}
      actual -> invalid({:map, field}, actual)
    end
  end

  def optional_map(value, field) do
    case Types.get(value, field) do
      nil -> {:ok, nil}
      map when is_map(map) -> {:ok, map}
      actual -> invalid({:map, field}, actual)
    end
  end

  def non_negative(value, field) do
    case Types.get(value, field) do
      integer when is_integer(integer) and integer >= 0 and integer <= @maximum_signed_64 ->
        {:ok, integer}

      actual ->
        invalid({:non_negative, field}, actual)
    end
  end

  def positive(value, field) do
    case Types.get(value, field) do
      integer when is_integer(integer) and integer > 0 and integer <= @maximum_signed_64 ->
        {:ok, integer}

      actual ->
        invalid({:positive, field}, actual)
    end
  end

  def unsigned(value, field) do
    case Types.get(value, field) do
      integer when is_integer(integer) and integer >= 0 and integer <= @maximum_unsigned_64 ->
        {:ok, integer}

      actual ->
        invalid({:unsigned, field}, actual)
    end
  end

  def positive_unsigned(value, field) do
    case Types.get(value, field) do
      integer when is_integer(integer) and integer > 0 and integer <= @maximum_unsigned_64 ->
        {:ok, integer}

      actual ->
        invalid({:positive_unsigned, field}, actual)
    end
  end

  def usage(value) when is_map(value) do
    Enum.reduce_while(@usage_fields, {:ok, %{}}, fn {field, atom}, {:ok, acc} ->
      case non_negative(value, field) do
        {:ok, number} -> {:cont, {:ok, Map.put(acc, atom, number)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def usage(value), do: invalid(:usage, value)

  def quality(value) when is_map(value) do
    with {:ok, exactness} <- bounded_binary(value, "exactness", 64),
         {:ok, freshness} <- bounded_binary(value, "freshness", 64),
         {:ok, coverage} <- bounded_binary(value, "coverage", 64),
         {:ok, pagination} <- bounded_binary(value, "pagination", 64) do
      {:ok,
       %{
         exactness: exactness,
         freshness: freshness,
         coverage: coverage,
         pagination: pagination
       }}
    end
  end

  def quality(value), do: invalid(:quality, value)

  def page(value) when is_map(value) do
    has_more = Types.get(value, "has_more")
    cursor = Types.get(value, "cursor")

    cond do
      not is_boolean(has_more) ->
        invalid(:page_has_more, has_more)

      cursor != nil and
          (not is_binary(cursor) or byte_size(cursor) > 4_096 or not String.valid?(cursor)) ->
        invalid(:page_cursor, cursor)

      is_binary(cursor) and not String.starts_with?(cursor, "fqc1_") ->
        invalid(:page_cursor, cursor)

      has_more != is_binary(cursor) ->
        invalid(:page_consistency, value)

      true ->
        {:ok, %{has_more: has_more, cursor: cursor}}
    end
  end

  def page(value), do: invalid(:page, value)

  def has_key?(map, key), do: Map.has_key?(map, key) or existing_atom_key?(map, key)

  def reject_key(map, key) do
    if has_key?(map, key), do: {:error, {:unexpected_field, key}}, else: :ok
  end

  def equal_count(value, value, _shape), do: :ok

  def equal_count(actual, expected, shape),
    do: invalid({shape, :result_records}, {actual, expected})

  def invalid(field, value), do: {:error, {:invalid_flow_query_response, field, value}}

  defp existing_atom_key?(map, key) do
    Map.has_key?(map, String.to_existing_atom(key))
  rescue
    ArgumentError -> false
  end
end
