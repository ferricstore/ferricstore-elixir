defmodule FerricStore.SDK.KV.MGetMerge do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.KV.MGetGroupCollector

  @spec merge(term(), non_neg_integer()) :: {:ok, list()} | {:error, term()}
  def merge(groups, count), do: do_merge(groups, count, nil)

  @spec merge(term(), non_neg_integer(), DeadlineBudget.t()) :: {:ok, list()} | {:error, term()}
  def merge(groups, count, %DeadlineBudget{} = budget), do: do_merge(groups, count, budget)

  defp do_merge(groups, count, budget) do
    missing = make_ref()
    values = :array.new(count, default: missing, fixed: true)

    case MGetGroupCollector.collect(groups, count, values, missing, budget) do
      {:ok, values, seen} -> finish(values, seen, count, budget)
      {:error, reason} -> {:error, reason}
    end
  end

  defp finish(values, count, count, budget) do
    with :ok <- ensure_active(budget),
         values = :array.to_list(values),
         :ok <- ensure_active(budget),
         do: {:ok, values}
  end

  defp finish(_values, actual, expected, _budget),
    do: {:error, {:missing_mget_indexes, %{actual_count: actual, expected_count: expected}}}

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
