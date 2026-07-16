defmodule FerricStore.Types.ValueNormalizer do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.Types.MapKeyNormalizer

  @deadline_check_interval 256
  @max_collection_items RequestLimits.max_batch_items()
  @max_value_depth 64

  def normalize(value), do: normalize_value(value, 0)

  def normalize(value, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, normalized, _until_check} <-
           normalize_budgeted_value(value, 0, budget, @deadline_check_interval),
         :ok <- DeadlineBudget.ensure_active(budget) do
      {:ok, normalized}
    end
  end

  defp normalize_value(value, depth) when is_map(value) do
    cond do
      depth >= @max_value_depth -> {:error, :value_nesting_too_deep}
      map_size(value) > @max_collection_items -> {:error, :collection_too_large}
      true -> normalize_map_entries(value, depth + 1)
    end
  end

  defp normalize_value(value, depth) when is_list(value) do
    if depth < @max_value_depth,
      do: normalize_list(value, [], depth + 1, 0),
      else: {:error, :value_nesting_too_deep}
  end

  defp normalize_value(value, _depth), do: {:ok, value}

  defp normalize_map_entries(value, child_depth) do
    Enum.reduce_while(value, {:ok, %{}}, &normalize_map_entry(&1, &2, child_depth))
  end

  defp normalize_map_entry({original_key, item}, {:ok, acc}, child_depth) do
    with {:ok, key} <- MapKeyNormalizer.normalize_key(original_key),
         :ok <- ensure_new_key(acc, key),
         {:ok, normalized} <- normalize_value(item, child_depth) do
      {:cont, {:ok, Map.put(acc, key, normalized)}}
    else
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp normalize_list([], normalized, _depth, _count), do: {:ok, Enum.reverse(normalized)}

  defp normalize_list([_item | _items], _normalized, _depth, @max_collection_items),
    do: {:error, :collection_too_large}

  defp normalize_list([item | items], normalized, depth, count) do
    case normalize_value(item, depth) do
      {:ok, item} -> normalize_list(items, [item | normalized], depth, count + 1)
      {:error, _reason} = error -> error
    end
  end

  defp normalize_list(_improper_tail, _normalized, _depth, _count),
    do: {:error, :improper_list}

  defp normalize_budgeted_value(value, depth, budget, until_check) when is_map(value) do
    cond do
      depth >= @max_value_depth -> {:error, :value_nesting_too_deep}
      map_size(value) > @max_collection_items -> {:error, :collection_too_large}
      true -> normalize_budgeted_map_entries(value, depth + 1, budget, until_check)
    end
  end

  defp normalize_budgeted_value(value, depth, budget, until_check) when is_list(value) do
    if depth < @max_value_depth,
      do: normalize_budgeted_list(value, [], depth + 1, 0, budget, until_check),
      else: {:error, :value_nesting_too_deep}
  end

  defp normalize_budgeted_value(value, _depth, _budget, until_check),
    do: {:ok, value, until_check}

  defp normalize_budgeted_map_entries(value, child_depth, budget, until_check) do
    Enum.reduce_while(value, {:ok, %{}, until_check}, fn
      {original_key, item}, {:ok, acc, current_check} ->
        with {:ok, next_check} <- advance_deadline_check(budget, current_check),
             {:ok, key} <- MapKeyNormalizer.normalize_key(original_key),
             :ok <- ensure_new_key(acc, key),
             {:ok, normalized_item, final_check} <-
               normalize_budgeted_value(item, child_depth, budget, next_check) do
          {:cont, {:ok, Map.put(acc, key, normalized_item), final_check}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp normalize_budgeted_list([], normalized, _depth, _count, _budget, until_check),
    do: {:ok, Enum.reverse(normalized), until_check}

  defp normalize_budgeted_list(
         [_item | _items],
         _normalized,
         _depth,
         @max_collection_items,
         _budget,
         _until_check
       ),
       do: {:error, :collection_too_large}

  defp normalize_budgeted_list(
         [item | items],
         normalized,
         depth,
         count,
         budget,
         until_check
       ) do
    with {:ok, next_check} <- advance_deadline_check(budget, until_check),
         {:ok, normalized_item, final_check} <-
           normalize_budgeted_value(item, depth, budget, next_check) do
      normalize_budgeted_list(
        items,
        [normalized_item | normalized],
        depth,
        count + 1,
        budget,
        final_check
      )
    end
  end

  defp normalize_budgeted_list(
         _improper_tail,
         _normalized,
         _depth,
         _count,
         _budget,
         _until_check
       ),
       do: {:error, :improper_list}

  defp advance_deadline_check(budget, 0) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, @deadline_check_interval - 1}
  end

  defp advance_deadline_check(_budget, until_check), do: {:ok, until_check - 1}

  defp ensure_new_key(acc, key) do
    if Map.has_key?(acc, key),
      do: {:error, {:duplicate_normalized_map_key, key}},
      else: :ok
  end
end
