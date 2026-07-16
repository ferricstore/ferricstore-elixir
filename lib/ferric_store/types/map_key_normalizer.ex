defmodule FerricStore.Types.MapKeyNormalizer do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}

  @deadline_check_interval 256
  @max_collection_items RequestLimits.max_command_items()

  def normalize(value) when is_map(value) and map_size(value) <= @max_collection_items do
    Enum.reduce_while(value, {:ok, %{}}, fn {original_key, item}, {:ok, normalized} ->
      with {:ok, key} <- normalize_key(original_key),
           :ok <- ensure_new_key(normalized, key) do
        {:cont, {:ok, Map.put(normalized, key, item)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  def normalize(value) when is_map(value), do: {:error, :collection_too_large}

  def normalize(value, %DeadlineBudget{} = budget) when is_map(value) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      if map_size(value) <= @max_collection_items,
        do: normalize_budgeted(value, budget),
        else: {:error, :collection_too_large}
    end
  end

  def normalize_key(key) when is_binary(key), do: {:ok, key}
  def normalize_key(key) when is_atom(key), do: {:ok, Atom.to_string(key)}
  def normalize_key(key), do: {:error, {:invalid_map_key, key}}

  defp normalize_budgeted(value, budget) do
    value
    |> Enum.reduce_while({:ok, %{}, 0}, fn entry, accumulator ->
      normalize_budgeted_entry(entry, accumulator, budget)
    end)
    |> finish(budget)
  end

  defp normalize_budgeted_entry(entry, {:ok, normalized, 0}, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok ->
        normalize_budgeted_entry(
          entry,
          {:ok, normalized, @deadline_check_interval},
          budget
        )

      {:error, _reason} = error ->
        {:halt, error}
    end
  end

  defp normalize_budgeted_entry(
         {original_key, item},
         {:ok, normalized, until_check},
         _budget
       ) do
    with {:ok, key} <- normalize_key(original_key),
         :ok <- ensure_new_key(normalized, key) do
      {:cont, {:ok, Map.put(normalized, key, item), until_check - 1}}
    else
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp ensure_new_key(normalized, key) do
    if Map.has_key?(normalized, key),
      do: {:error, {:duplicate_normalized_map_key, key}},
      else: :ok
  end

  defp finish({:ok, normalized, _count}, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, normalized}
  end

  defp finish({:error, _reason} = error, _budget), do: error
end
