defmodule FerricStore.BoundedList do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @budget_check_interval 256

  @type admission_error :: {:limit_exceeded, pos_integer()} | :improper_list

  @spec count(list(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, admission_error()}
  def count(items, limit) when is_list(items) and is_integer(limit) and limit >= 0 do
    do_count(items, limit, 0)
  end

  @spec count(list(), non_neg_integer(), DeadlineBudget.t()) ::
          {:ok, non_neg_integer()} | {:error, admission_error() | :timeout}
  def count(items, limit, %DeadlineBudget{} = budget)
      when is_list(items) and is_integer(limit) and limit >= 0 do
    do_budgeted_count(items, limit, 0, 0, budget)
  end

  defp do_count([], _limit, count), do: {:ok, count}

  defp do_count([_item | rest], limit, count) when count < limit,
    do: do_count(rest, limit, count + 1)

  defp do_count([_item | _rest], limit, limit),
    do: {:error, {:limit_exceeded, limit + 1}}

  defp do_count(_improper_tail, _limit, _count), do: {:error, :improper_list}

  defp do_budgeted_count(items, limit, count, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      do_budgeted_count(items, limit, count, @budget_check_interval, budget)
    end
  end

  defp do_budgeted_count([], _limit, count, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, count}
  end

  defp do_budgeted_count([_item | rest], limit, count, until_check, budget)
       when count < limit do
    do_budgeted_count(rest, limit, count + 1, until_check - 1, budget)
  end

  defp do_budgeted_count([_item | _rest], limit, limit, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: {:error, {:limit_exceeded, limit + 1}}
  end

  defp do_budgeted_count(_improper_tail, _limit, _count, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:error, :improper_list}
  end

  @spec map(list(), non_neg_integer(), (term() -> term())) ::
          {:ok, list()} | {:error, admission_error()}
  def map(items, limit, mapper)
      when is_list(items) and is_integer(limit) and limit >= 0 and is_function(mapper, 1) do
    case map_with_count(items, limit, mapper) do
      {:ok, _count, mapped} -> {:ok, mapped}
      {:error, _reason} = error -> error
    end
  end

  @spec map_with_count(list(), non_neg_integer(), (term() -> term())) ::
          {:ok, non_neg_integer(), list()} | {:error, admission_error()}
  def map_with_count(items, limit, mapper)
      when is_list(items) and is_integer(limit) and limit >= 0 and is_function(mapper, 1) do
    with {:ok, count} <- count(items, limit) do
      {:ok, count, Enum.map(items, mapper)}
    end
  end

  @spec map_result_with_count(
          list(),
          non_neg_integer(),
          (term() -> {:ok, term()} | {:error, term()})
        ) ::
          {:ok, non_neg_integer(), list()}
          | {:error, admission_error() | term()}
  def map_result_with_count(items, limit, mapper)
      when is_list(items) and is_integer(limit) and limit >= 0 and is_function(mapper, 1) do
    case count(items, limit) do
      {:ok, count} -> map_results(items, mapper, count, [])
      {:error, _reason} = error -> error
    end
  end

  @spec map_result_with_count(
          list(),
          non_neg_integer(),
          (term() -> {:ok, term()} | {:error, term()}),
          DeadlineBudget.t()
        ) ::
          {:ok, non_neg_integer(), list()}
          | {:error, admission_error() | :timeout | term()}
  def map_result_with_count(items, limit, mapper, %DeadlineBudget{} = budget)
      when is_list(items) and is_integer(limit) and limit >= 0 and is_function(mapper, 1) do
    case count(items, limit, budget) do
      {:ok, count} -> budgeted_map_results(items, mapper, count, [], 0, budget)
      {:error, _reason} = error -> error
    end
  end

  defp map_results([], _mapper, count, mapped),
    do: {:ok, count, Enum.reverse(mapped)}

  defp map_results([item | items], mapper, count, mapped) do
    case mapper.(item) do
      {:ok, value} -> map_results(items, mapper, count, [value | mapped])
      {:error, _reason} = error -> error
    end
  end

  defp budgeted_map_results(items, mapper, count, mapped, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      budgeted_map_results(items, mapper, count, mapped, @budget_check_interval, budget)
    end
  end

  defp budgeted_map_results([], _mapper, count, mapped, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         mapped = Enum.reverse(mapped),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, count, mapped}
  end

  defp budgeted_map_results([item | items], mapper, count, mapped, until_check, budget) do
    case mapper.(item) do
      {:ok, value} ->
        budgeted_map_results(items, mapper, count, [value | mapped], until_check - 1, budget)

      {:error, _reason} = error ->
        error
    end
  end
end
