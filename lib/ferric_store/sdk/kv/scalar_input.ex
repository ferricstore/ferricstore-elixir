defmodule FerricStore.SDK.KV.ScalarInput do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain,
    only: [is_signed_64_integer: 1]

  @spec binary(term(), atom(), atom()) ::
          {:ok, binary()} | {:error, {:invalid_kv_input, map()}}
  def binary(value, _operation, _field) when is_binary(value), do: {:ok, value}

  def binary(_value, operation, field),
    do: invalid_input(operation, field, :expected_binary)

  @spec nonempty_binary(term(), atom(), atom()) ::
          {:ok, binary()} | {:error, {:invalid_kv_input, map()}}
  def nonempty_binary(value, _operation, _field) when is_binary(value) and value != "",
    do: {:ok, value}

  def nonempty_binary(_value, operation, field),
    do: invalid_input(operation, field, :expected_nonempty_binary)

  @spec integer(term(), atom(), atom()) ::
          {:ok, integer()} | {:error, {:invalid_kv_input, map()}}
  def integer(value, _operation, _field) when is_signed_64_integer(value), do: {:ok, value}

  def integer(value, operation, field) when is_integer(value),
    do: invalid_input(operation, field, :outside_signed_64_domain)

  def integer(_value, operation, field),
    do: invalid_input(operation, field, :expected_integer)

  @spec non_negative_integer(term(), atom(), atom()) ::
          {:ok, non_neg_integer()} | {:error, {:invalid_kv_input, map()}}
  def non_negative_integer(value, _operation, _field)
      when is_signed_64_integer(value) and value >= 0,
      do: {:ok, value}

  def non_negative_integer(value, operation, field) when is_integer(value) do
    reason =
      if is_signed_64_integer(value),
        do: :expected_non_negative_integer,
        else: :outside_signed_64_domain

    invalid_input(operation, field, reason)
  end

  def non_negative_integer(_value, operation, field),
    do: invalid_input(operation, field, :expected_non_negative_integer)

  @spec positive_integer(term(), atom(), atom()) ::
          {:ok, pos_integer()} | {:error, {:invalid_kv_input, map()}}
  def positive_integer(value, _operation, _field)
      when is_signed_64_integer(value) and value > 0,
      do: {:ok, value}

  def positive_integer(value, operation, field) when is_integer(value) do
    reason =
      if is_signed_64_integer(value),
        do: :expected_positive_integer,
        else: :outside_signed_64_domain

    invalid_input(operation, field, reason)
  end

  def positive_integer(_value, operation, field),
    do: invalid_input(operation, field, :expected_positive_integer)

  @spec optional_boolean(term(), atom(), atom()) ::
          {:ok, boolean() | nil} | {:error, {:invalid_kv_input, map()}}
  def optional_boolean(nil, _operation, _field), do: {:ok, nil}
  def optional_boolean(value, _operation, _field) when is_boolean(value), do: {:ok, value}

  def optional_boolean(_value, operation, field),
    do: invalid_input(operation, field, :expected_boolean)

  defp invalid_input(operation, field, reason) do
    {:error, {:invalid_kv_input, %{operation: operation, field: field, reason: reason}}}
  end
end
