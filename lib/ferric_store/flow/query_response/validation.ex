defmodule FerricStore.Flow.QueryResponse.Validation do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.PageValidation
  alias FerricStore.Types

  @maximum_signed_64 9_223_372_036_854_775_807
  @maximum_unsigned_64 18_446_744_073_709_551_615

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

  def has_key?(map, key), do: Map.has_key?(map, key) or existing_atom_key?(map, key)

  def reject_key(map, key) do
    if has_key?(map, key), do: {:error, {:unexpected_field, key}}, else: :ok
  end

  def equal_count(value, value, _shape), do: :ok

  def equal_count(actual, expected, shape),
    do: invalid({shape, :result_records}, {actual, expected})

  def not_greater(actual, maximum, _shape) when actual <= maximum, do: :ok

  def not_greater(actual, maximum, shape),
    do: invalid({shape, :scanned_entries}, {actual, maximum})

  def invalid(field, value), do: {:error, {:invalid_flow_query_response, field, value}}

  defp existing_atom_key?(map, key) do
    Map.has_key?(map, String.to_existing_atom(key))
  rescue
    ArgumentError -> false
  end

  defdelegate page(value), to: PageValidation, as: :validate
end
