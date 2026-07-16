defmodule FerricStore.Flow.Options.CollectionScan do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @deadline_check_interval 256

  @spec validate(term(), non_neg_integer(), (term() -> boolean()), DeadlineBudget.t() | nil) ::
          {:ok, non_neg_integer()}
          | {:error, :expected_list | :invalid_item | :too_large | :timeout}
  def validate(items, limit, predicate, budget)
      when is_integer(limit) and limit >= 0 and is_function(predicate, 1) do
    with :ok <- active(budget), do: scan(items, limit, predicate, budget, 0, 0)
  end

  defp scan([], _limit, _predicate, budget, count, _until_check) do
    with :ok <- active(budget), do: {:ok, count}
  end

  defp scan([_item | _rest], limit, _predicate, budget, limit, _until_check) do
    with :ok <- active(budget), do: {:error, :too_large}
  end

  defp scan([item | rest], limit, predicate, budget, count, 0) do
    with :ok <- active(budget),
         do: scan([item | rest], limit, predicate, budget, count, @deadline_check_interval)
  end

  defp scan([item | rest], limit, predicate, budget, count, until_check) do
    if predicate.(item),
      do: scan(rest, limit, predicate, budget, count + 1, until_check - 1),
      else: {:error, :invalid_item}
  end

  defp scan(_value, _limit, _predicate, budget, _count, _until_check) do
    with :ok <- active(budget),
         do: {:error, :expected_list}
  end

  defp active(nil), do: :ok
  defp active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
