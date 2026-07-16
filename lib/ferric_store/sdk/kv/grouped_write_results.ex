defmodule FerricStore.SDK.KV.GroupedWriteResults do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.KV.{GroupCoverage, GroupedWriteError}

  @spec del(term(), non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def del(groups, expected_items), do: do_del(groups, expected_items, nil)

  def del(groups, expected_items, %DeadlineBudget{} = budget),
    do: do_del(groups, expected_items, budget)

  defp do_del(groups, expected_items, budget) when is_list(groups) do
    groups
    |> collect_del_groups(0, GroupCoverage.new(expected_items), budget)
    |> validate_del_coverage(budget)
  end

  defp do_del(_groups, _expected_items, _budget),
    do: {:error, {:invalid_del_group_response, GroupedWriteError.invalid_group()}}

  @spec mset(term(), non_neg_integer()) :: {:ok, :ok} | {:error, term()}
  def mset(groups, expected_items), do: do_mset(groups, expected_items, nil)

  def mset(groups, expected_items, %DeadlineBudget{} = budget),
    do: do_mset(groups, expected_items, budget)

  defp do_mset(groups, expected_items, budget) when is_list(groups) do
    groups
    |> collect_mset_groups(GroupCoverage.new(expected_items), budget)
    |> validate_mset_coverage(budget)
  end

  defp do_mset(_groups, _expected_items, _budget),
    do: {:error, {:invalid_mset_group_response, GroupedWriteError.invalid_group()}}

  defp collect_del_groups([], total, coverage, budget) do
    with :ok <- ensure_active(budget), do: {:ok, total, coverage}
  end

  defp collect_del_groups(
         [%{indexes: indexes, value: value} | groups],
         total,
         coverage,
         budget
       )
       when is_list(indexes) and is_integer(value) and value >= 0 do
    case add_coverage(coverage, indexes, budget) do
      {:ok, coverage, group_items} when value <= group_items ->
        collect_del_groups(groups, total + value, coverage, budget)

      {:ok, _coverage, group_items} ->
        {:error,
         {:invalid_del_group_response,
          %{reason: :count_exceeds_group_items, value: value, group_items: group_items}}}

      {:error, details} ->
        GroupedWriteError.coverage(:del, details)
    end
  end

  defp collect_del_groups([group | _groups], _total, _coverage, _budget),
    do: {:error, {:invalid_del_group_response, GroupedWriteError.invalid_value(group)}}

  defp collect_del_groups(improper_tail, _total, _coverage, _budget),
    do: {:error, {:invalid_del_group_response, GroupedWriteError.invalid_value(improper_tail)}}

  defp collect_mset_groups([], coverage, budget) do
    with :ok <- ensure_active(budget), do: {:ok, coverage}
  end

  defp collect_mset_groups(
         [%{indexes: indexes, value: "OK"} | groups],
         coverage,
         budget
       )
       when is_list(indexes) do
    case add_coverage(coverage, indexes, budget) do
      {:ok, coverage, _group_items} -> collect_mset_groups(groups, coverage, budget)
      {:error, details} -> GroupedWriteError.coverage(:mset, details)
    end
  end

  defp collect_mset_groups([group | _groups], _coverage, _budget),
    do: {:error, {:invalid_mset_group_response, GroupedWriteError.invalid_value(group)}}

  defp collect_mset_groups(improper_tail, _coverage, _budget),
    do: {:error, {:invalid_mset_group_response, GroupedWriteError.invalid_value(improper_tail)}}

  defp validate_del_coverage({:ok, total, coverage}, budget) do
    with :ok <- ensure_active(budget) do
      case GroupCoverage.complete(coverage) do
        :ok -> {:ok, total}
        {:error, details} -> {:error, {:invalid_del_group_response, details}}
      end
    end
  end

  defp validate_del_coverage({:error, _reason} = error, _budget), do: error

  defp validate_mset_coverage({:ok, coverage}, budget) do
    with :ok <- ensure_active(budget) do
      case GroupCoverage.complete(coverage) do
        :ok -> {:ok, :ok}
        {:error, details} -> {:error, {:invalid_mset_group_response, details}}
      end
    end
  end

  defp validate_mset_coverage({:error, _reason} = error, _budget), do: error

  defp add_coverage(coverage, indexes, nil), do: GroupCoverage.add(coverage, indexes)

  defp add_coverage(coverage, indexes, %DeadlineBudget{} = budget),
    do: GroupCoverage.add(coverage, indexes, budget)

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
