defmodule FerricStore.SDK.KV.ScalarResponseValue do
  @moduledoc false

  def set("OK"), do: {:ok, :ok}
  def set(nil), do: {:ok, nil}
  def set(_value), do: {:error, :unexpected_value}

  def ok("OK"), do: {:ok, :ok}
  def ok(_value), do: {:error, :unexpected_value}

  def boolean_or_nil(value) when is_boolean(value) or is_nil(value), do: {:ok, value}
  def boolean_or_nil(_value), do: {:error, :expected_boolean_or_nil}

  def boolean(value) when is_boolean(value), do: {:ok, value}
  def boolean(_value), do: {:error, :expected_boolean}

  def one(1), do: {:ok, 1}
  def one(_value), do: {:error, :expected_one}

  def non_negative_integer(value) when is_integer(value) and value >= 0, do: {:ok, value}
  def non_negative_integer(_value), do: {:error, :expected_non_negative_integer}

  def pop(value) when is_binary(value) or is_nil(value), do: {:ok, value}
  def pop(_value), do: {:error, :expected_binary_or_nil}
end
