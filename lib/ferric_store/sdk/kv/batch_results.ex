defmodule FerricStore.SDK.KV.BatchResults do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.KV.GroupedWriteResults
  alias FerricStore.SDK.KV.MGetDenseValidation
  alias FerricStore.SDK.KV.{MGetGroupValidation, MGetMerge, ResultCount}

  @spec mget([map()], non_neg_integer()) :: {:ok, list()} | {:error, term()}
  def mget(groups, count),
    do: with(:ok <- ResultCount.validate(:mget, count), do: do_mget(groups, count, nil))

  @spec mget([map()], non_neg_integer(), DeadlineBudget.t()) ::
          {:ok, list()} | {:error, term()}
  def mget(groups, count, %DeadlineBudget{} = budget),
    do: with(:ok <- ResultCount.validate(:mget, count), do: do_mget(groups, count, budget))

  defp do_mget([%{indexes: [], value: []}], _count, _budget),
    do: {:error, {:invalid_mget_group_response, %{reason: :empty_indexes}}}

  defp do_mget([%{indexes: indexes, value: values}] = groups, count, budget)
       when is_list(indexes) and is_list(values) do
    case classify_dense(indexes, values, count, budget) do
      :dense ->
        {:ok, values}

      :timeout ->
        {:error, :timeout}

      {:mismatched, expected, actual} ->
        {:error, MGetGroupValidation.size_error(expected, actual)}

      :improper_indexes ->
        {:error, {:invalid_mget_group_response, %{reason: :improper_indexes}}}

      :improper_values ->
        {:error, {:invalid_mget_group_response, %{reason: :improper_values}}}

      {:invalid_value, index} ->
        invalid_mget_value(index)

      :reorder ->
        merge(groups, count, budget)
    end
  end

  defp do_mget(groups, _count, _budget) when not is_list(groups),
    do: {:error, {:invalid_mget_group_response, invalid_group()}}

  defp do_mget(groups, count, budget), do: merge(groups, count, budget)

  @spec del([map()], non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def del(groups, expected_items) do
    with :ok <- ResultCount.validate(:del, expected_items),
         do: GroupedWriteResults.del(groups, expected_items)
  end

  def del(groups, expected_items, %DeadlineBudget{} = budget) do
    with :ok <- ResultCount.validate(:del, expected_items),
         do: GroupedWriteResults.del(groups, expected_items, budget)
  end

  @spec mset([map()], non_neg_integer()) :: {:ok, :ok} | {:error, term()}
  def mset(groups, expected_items) do
    with :ok <- ResultCount.validate(:mset, expected_items),
         do: GroupedWriteResults.mset(groups, expected_items)
  end

  def mset(groups, expected_items, %DeadlineBudget{} = budget) do
    with :ok <- ResultCount.validate(:mset, expected_items),
         do: GroupedWriteResults.mset(groups, expected_items, budget)
  end

  defp invalid_mget_value(index),
    do: {:error, {:invalid_mget_group_response, %{reason: :expected_binary_or_nil, index: index}}}

  defp invalid_group, do: %{reason: :unexpected_group_shape}

  defp classify_dense(indexes, values, count, nil),
    do: MGetDenseValidation.classify(indexes, values, count)

  defp classify_dense(indexes, values, count, %DeadlineBudget{} = budget),
    do: MGetDenseValidation.classify(indexes, values, count, budget)

  defp merge(groups, count, nil), do: MGetMerge.merge(groups, count)

  defp merge(groups, count, %DeadlineBudget{} = budget),
    do: MGetMerge.merge(groups, count, budget)
end
