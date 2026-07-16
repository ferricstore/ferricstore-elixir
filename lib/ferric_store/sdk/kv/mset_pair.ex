defmodule FerricStore.SDK.KV.MSetPair do
  @moduledoc false

  @type normalized :: {binary(), binary()}

  @spec normalize(term()) :: {:ok, normalized()} | {:error, {:invalid_mset_pair, term()}}
  def normalize({key, value} = pair), do: validate(pair, key, value)
  def normalize([key, value] = pair), do: validate(pair, key, value)

  def normalize(%{"key" => key, "value" => value} = pair) when map_size(pair) == 2,
    do: validate(pair, key, value)

  def normalize(%{key: key, value: value} = pair) when map_size(pair) == 2,
    do: validate(pair, key, value)

  def normalize(pair), do: {:error, {:invalid_mset_pair, pair}}

  defp validate(_pair, key, value) when is_binary(key) and is_binary(value),
    do: {:ok, {key, value}}

  defp validate(_pair, key, _value) when is_binary(key),
    do: {:error, {:invalid_mset_pair, %{reason: :expected_binary_value}}}

  defp validate(pair, _key, _value), do: {:error, {:invalid_mset_pair, pair}}
end
