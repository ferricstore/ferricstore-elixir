defmodule FerricStore.Types do
  @moduledoc """
  Helpers for reading and normalizing native protocol maps.
  """

  alias FerricStore.{DeadlineBudget, FailureFormatter, RequestLimits}
  alias FerricStore.Types.{MapKeyNormalizer, ValueNormalizer}

  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  def get(map, key, default \\ nil)

  def get(map, key, default) when is_map(map) and is_atom(key),
    do: get(map, Atom.to_string(key), default)

  def get(map, key, default) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> get_existing_atom_key(map, key, default)
    end
  end

  def get(_map, _key, default), do: default

  def normalize_map(value) when is_map(value) or is_list(value) do
    case normalize_map_result(value) do
      {:ok, normalized} -> normalized
      {:error, reason} -> raise_normalization_error(reason)
    end
  end

  def normalize_map(value), do: value

  @doc false
  @spec normalize_map_keys(map()) :: map()
  def normalize_map_keys(value) when is_map(value) do
    case normalize_map_keys_result(value) do
      {:ok, normalized} -> normalized
      {:error, reason} -> raise_normalization_error(reason)
    end
  end

  @doc false
  def normalize_map_keys_result(value) when is_map(value), do: MapKeyNormalizer.normalize(value)

  @doc false
  def normalize_map_keys_result(value, %DeadlineBudget{} = budget) when is_map(value),
    do: MapKeyNormalizer.normalize(value, budget)

  def normalize_map_result(value), do: ValueNormalizer.normalize(value)

  @doc false
  def normalize_map_result(value, %DeadlineBudget{} = budget),
    do: ValueNormalizer.normalize(value, budget)

  defp raise_normalization_error({:duplicate_normalized_map_key, key}),
    do: raise(ArgumentError, "duplicate normalized map key #{inspect(key)}")

  defp raise_normalization_error(:improper_list),
    do: raise(ArgumentError, "cannot normalize an improper list")

  defp raise_normalization_error({:invalid_map_key, key}),
    do: raise(ArgumentError, "cannot normalize map key #{FailureFormatter.inspect_term(key)}")

  defp raise_normalization_error(:value_nesting_too_deep),
    do: raise(ArgumentError, "native protocol value nesting exceeds #{@max_value_depth} levels")

  defp raise_normalization_error(:collection_too_large),
    do:
      raise(
        ArgumentError,
        "native protocol collection exceeds #{@max_collection_items} items"
      )

  defp get_existing_atom_key(map, key, default) do
    Map.get(map, String.to_existing_atom(key), default)
  rescue
    ArgumentError -> default
  end
end
