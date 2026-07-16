defmodule FerricStore.SDK.KV.MGetGroupCollector do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  alias FerricStore.SDK.KV.{
    MGetGroupValidation,
    MGetTailLength,
    ResponseValue
  }

  @deadline_check_interval 256

  @spec collect(term(), non_neg_integer(), :array.array(), reference(), DeadlineBudget.t() | nil) ::
          {:ok, :array.array(), non_neg_integer()} | {:error, term()}
  def collect(groups, count, values, missing, budget),
    do: collect_groups(groups, count, values, missing, 0, budget)

  defp collect_groups([], _count, values, _missing, seen, budget) do
    with :ok <- ensure_active(budget), do: {:ok, values, seen}
  end

  defp collect_groups(
         [%{indexes: indexes, value: values} | groups],
         count,
         collected,
         missing,
         seen,
         budget
       )
       when is_list(indexes) and is_list(values) do
    with :ok <- ensure_active(budget),
         :ok <- validate_nonempty_group(indexes, values),
         {:ok, collected, seen} <-
           collect_group(indexes, values, count, collected, missing, seen, 0, budget) do
      collect_groups(groups, count, collected, missing, seen, budget)
    end
  end

  defp collect_groups(_invalid, _count, _values, _missing, _seen, _budget),
    do: invalid(:unexpected_group_shape)

  defp validate_nonempty_group([], []), do: invalid(:empty_indexes)
  defp validate_nonempty_group(_indexes, _values), do: :ok

  defp collect_group([], [], _count, values, _missing, seen, _group_count, budget) do
    with :ok <- ensure_active(budget), do: {:ok, values, seen}
  end

  defp collect_group(indexes, values, count, collected, missing, seen, group_count, budget)
       when rem(group_count, @deadline_check_interval) == 0 do
    with :ok <- ensure_active(budget) do
      collect_group_item(
        indexes,
        values,
        count,
        collected,
        missing,
        seen,
        group_count,
        budget
      )
    end
  end

  defp collect_group(indexes, values, count, collected, missing, seen, group_count, budget),
    do:
      collect_group_item(
        indexes,
        values,
        count,
        collected,
        missing,
        seen,
        group_count,
        budget
      )

  defp collect_group_item(
         [index | indexes],
         [value | values],
         count,
         collected,
         missing,
         seen,
         group_count,
         budget
       ) do
    cond do
      not is_integer(index) or index < 0 or index >= count ->
        {:error, {:invalid_mget_index, %{expected_range: {0, count - 1}}}}

      not ResponseValue.binary_or_nil?(value) ->
        invalid(:expected_binary_or_nil, %{index: index})

      :array.get(index, collected) != missing ->
        {:error, {:duplicate_mget_index, %{index: index}}}

      true ->
        collect_group(
          indexes,
          values,
          count,
          :array.set(index, value, collected),
          missing,
          seen + 1,
          group_count + 1,
          budget
        )
    end
  end

  defp collect_group_item([], values, _count, _collected, _missing, _seen, group_count, budget) do
    case MGetTailLength.count(values, group_count, budget) do
      {:ok, actual} -> {:error, MGetGroupValidation.size_error(group_count, actual)}
      :improper -> invalid(:improper_values)
      :timeout -> {:error, :timeout}
    end
  end

  defp collect_group_item(indexes, [], _count, _collected, _missing, _seen, group_count, budget) do
    case MGetTailLength.count(indexes, group_count, budget) do
      {:ok, expected} -> {:error, MGetGroupValidation.size_error(expected, group_count)}
      :improper -> invalid(:improper_indexes)
      :timeout -> {:error, :timeout}
    end
  end

  defp collect_group_item(
         indexes,
         _values,
         _count,
         _collected,
         _missing,
         _seen,
         _group_count,
         _budget
       )
       when not is_list(indexes),
       do: invalid(:improper_indexes)

  defp collect_group_item(
         _indexes,
         values,
         _count,
         _collected,
         _missing,
         _seen,
         _group_count,
         _budget
       )
       when not is_list(values),
       do: invalid(:improper_values)

  defp collect_group_item(
         _indexes,
         _values,
         _count,
         _collected,
         _missing,
         _seen,
         _group_count,
         _budget
       ),
       do: invalid(:unexpected_group_shape)

  defp invalid(reason, details \\ %{}),
    do: {:error, {:invalid_mget_group_response, Map.put(details, :reason, reason)}}

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
