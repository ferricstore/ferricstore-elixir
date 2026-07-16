defmodule FerricStore.SDK.ManagementPairNormalizer do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  alias FerricStore.Protocol.CommandName
  alias FerricStore.SDK.ManagementInputError

  @spec normalize(term(), atom(), atom(), non_neg_integer(), MapSet.t(), list()) ::
          {:ok, MapSet.t(), list()} | {:error, term()}
  def normalize({key, value}, operation, field, index, seen, args) do
    with {:ok, key} <- pair_key(key, operation, field, index),
         :ok <- unique_key(key, seen, operation, field),
         {:ok, args} <- put_arg(key, value, args, operation, field, index) do
      {:ok, MapSet.put(seen, key), args}
    end
  end

  def normalize(value, operation, field, index, _seen, _args),
    do:
      ManagementInputError.invalid(operation, field, :expected_pair, %{index: index, value: value})

  defp pair_key(key, operation, field, index) when is_atom(key) do
    key
    |> Atom.to_string()
    |> normalize_key(key, operation, field, index)
  end

  defp pair_key(key, operation, field, index) when is_binary(key),
    do: normalize_key(key, key, operation, field, index)

  defp pair_key(key, operation, field, index),
    do: ManagementInputError.invalid(operation, field, :invalid_key, %{index: index, value: key})

  defp normalize_key(key, original, operation, field, index) do
    case CommandName.normalize(key) do
      {:ok, normalized} ->
        {:ok, normalized}

      {:error, _reason} ->
        ManagementInputError.invalid(operation, field, :invalid_key, %{
          index: index,
          value: original
        })
    end
  end

  defp unique_key(key, seen, operation, field) do
    if MapSet.member?(seen, key),
      do: ManagementInputError.invalid(operation, field, :duplicate_keys, %{keys: [key]}),
      else: :ok
  end

  defp put_arg(_key, nil, args, _operation, _field, _index), do: {:ok, args}

  defp put_arg(key, value, args, operation, field, index) do
    case pair_value(value) do
      {:ok, value} -> {:ok, [value, key | args]}
      :error -> ManagementInputError.invalid(operation, field, :invalid_value, %{index: index})
    end
  end

  defp pair_value(value) when is_binary(value), do: {:ok, value}
  defp pair_value(value) when is_boolean(value), do: {:ok, value}
  defp pair_value(value) when is_atom(value), do: {:ok, Atom.to_string(value)}
  defp pair_value(value) when is_signed_64_integer(value), do: {:ok, value}
  defp pair_value(value) when is_float(value), do: {:ok, value}
  defp pair_value(_value), do: :error
end
