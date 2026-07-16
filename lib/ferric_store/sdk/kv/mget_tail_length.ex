defmodule FerricStore.SDK.KV.MGetTailLength do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @deadline_check_interval 256

  @spec count(term(), non_neg_integer(), DeadlineBudget.t() | nil) ::
          {:ok, non_neg_integer()} | :improper | :timeout
  def count(items, initial, budget), do: count(items, initial, 0, budget)

  defp count([], count, _check, budget) do
    case ensure_active(budget) do
      :ok -> {:ok, count}
      {:error, :timeout} -> :timeout
    end
  end

  defp count(items, count, 0, budget) do
    case ensure_active(budget) do
      :ok -> count(items, count, @deadline_check_interval, budget)
      {:error, :timeout} -> :timeout
    end
  end

  defp count([_item | items], count, check, budget),
    do: count(items, count + 1, check - 1, budget)

  defp count(_improper_tail, _count, _check, _budget), do: :improper

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
