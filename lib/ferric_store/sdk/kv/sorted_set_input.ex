defmodule FerricStore.SDK.KV.SortedSetInput do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_native_score: 1]

  alias FerricStore.{DeadlineBudget, RequestLimits}

  @max_batch_items RequestLimits.max_batch_items()
  @deadline_check_interval 256

  @spec zadd_items(term(), DeadlineBudget.t()) ::
          {:ok, non_neg_integer(), list()}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_zadd_item, term()}}
  def zadd_items(items, %DeadlineBudget{} = budget) when is_list(items) do
    normalize_items(items, 0, [], 0, budget)
  end

  def zadd_items(value, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid_input(:expected_list, %{value: value})
  end

  defp normalize_items([], count, normalized, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, count, Enum.reverse(normalized)}
  end

  defp normalize_items(
         [_item | _items],
         @max_batch_items,
         _normalized,
         _until_check,
         budget
       ) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: batch_too_large(@max_batch_items + 1)
  end

  defp normalize_items(items, index, normalized, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      normalize_items(items, index, normalized, @deadline_check_interval, budget)
    end
  end

  defp normalize_items([item | items], index, normalized, until_check, budget) do
    case item_payload(item) do
      {:ok, normalized_item} ->
        normalize_items(
          items,
          index + 1,
          [normalized_item | normalized],
          until_check - 1,
          budget
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_items(_improper_tail, _index, _normalized, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid_input(:improper_list)
  end

  defp item_payload([score, member]) when is_native_score(score) and is_binary(member),
    do: {:ok, [score, member]}

  defp item_payload({score, member}) when is_native_score(score) and is_binary(member),
    do: {:ok, [score, member]}

  defp item_payload(%{"score" => score, "member" => member} = item)
       when is_native_score(score) and is_binary(member) and map_size(item) == 2,
       do: {:ok, [score, member]}

  defp item_payload(%{score: score, member: member} = item)
       when is_native_score(score) and is_binary(member) and map_size(item) == 2,
       do: {:ok, [score, member]}

  defp item_payload(item), do: {:error, {:invalid_zadd_item, item}}

  defp batch_too_large(observed),
    do: {:error, {:batch_too_large, %{items: observed, limit: @max_batch_items}}}

  defp invalid_input(reason, details \\ %{}) do
    {:error,
     {:invalid_kv_input, Map.merge(%{operation: :zadd, field: :items, reason: reason}, details)}}
  end
end
