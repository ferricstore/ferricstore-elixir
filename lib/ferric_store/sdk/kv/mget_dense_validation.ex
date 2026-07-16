defmodule FerricStore.SDK.KV.MGetDenseValidation do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @deadline_check_interval 256

  @type result ::
          :dense
          | :reorder
          | :improper_indexes
          | :improper_values
          | :timeout
          | {:invalid_value, non_neg_integer()}
          | {:mismatched, non_neg_integer(), non_neg_integer()}

  @spec classify(list(), list(), non_neg_integer()) :: result()
  def classify(indexes, values, count), do: classify(indexes, values, 0, count, 0, nil)

  @spec classify(list(), list(), non_neg_integer(), DeadlineBudget.t()) :: result()
  def classify(indexes, values, count, %DeadlineBudget{} = budget),
    do: classify(indexes, values, 0, count, 0, budget)

  defp classify([], [], count, count, _check, budget), do: active_result(:dense, budget)
  defp classify([], [], _index, _count, _check, budget), do: active_result(:reorder, budget)

  defp classify([], values, index, _count, check, budget) do
    case list_length(values, index, check, budget) do
      {:ok, actual} -> {:mismatched, index, actual}
      :improper -> :improper_values
      :timeout -> :timeout
    end
  end

  defp classify(indexes, [], actual, _count, check, budget) do
    case list_length(indexes, actual, check, budget) do
      {:ok, expected} -> {:mismatched, expected, actual}
      :improper -> :improper_indexes
      :timeout -> :timeout
    end
  end

  defp classify(indexes, values, index, count, 0, budget) do
    case ensure_active(budget) do
      :ok -> classify(indexes, values, index, count, @deadline_check_interval, budget)
      {:error, :timeout} -> :timeout
    end
  end

  defp classify([index | indexes], [value | values], index, count, check, budget)
       when is_binary(value) or is_nil(value),
       do: classify(indexes, values, index + 1, count, check - 1, budget)

  defp classify([index | _indexes], [_value | _values], index, _count, _check, _budget),
    do: {:invalid_value, index}

  defp classify(_indexes, _values, _index, _count, _check, budget),
    do: active_result(:reorder, budget)

  defp list_length([], count, _check, budget), do: active_length(count, budget)

  defp list_length(items, count, 0, budget) do
    case ensure_active(budget) do
      :ok -> list_length(items, count, @deadline_check_interval, budget)
      {:error, :timeout} -> :timeout
    end
  end

  defp list_length([_item | items], count, check, budget),
    do: list_length(items, count + 1, check - 1, budget)

  defp list_length(_improper_tail, _count, _check, _budget), do: :improper

  defp active_result(result, budget) do
    case ensure_active(budget) do
      :ok -> result
      {:error, :timeout} -> :timeout
    end
  end

  defp active_length(count, budget) do
    case ensure_active(budget) do
      :ok -> {:ok, count}
      {:error, :timeout} -> :timeout
    end
  end

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
