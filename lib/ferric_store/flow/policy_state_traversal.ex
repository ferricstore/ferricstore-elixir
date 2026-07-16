defmodule FerricStore.Flow.PolicyStateTraversal do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @deadline_check_interval 256

  @spec reduce(
          map() | list(),
          accumulator,
          DeadlineBudget.t(),
          ({term(), term()}, accumulator ->
             {:ok, accumulator} | {:error, term()})
        ) :: {:ok, accumulator} | {:error, term()}
        when accumulator: term()
  def reduce(states, accumulator, %DeadlineBudget{} = budget, reducer)
      when is_map(states) and is_function(reducer, 2) do
    states
    |> Enum.reduce_while({:ok, accumulator, 0}, fn entry, state ->
      reduce_entry(entry, state, budget, reducer)
    end)
    |> finish(budget)
  end

  def reduce(states, accumulator, %DeadlineBudget{} = budget, reducer)
      when is_list(states) and is_function(reducer, 2) do
    reduce_list(states, accumulator, 0, budget, reducer)
  end

  defp reduce_list(entries, accumulator, 0, budget, reducer) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      reduce_list(entries, accumulator, @deadline_check_interval, budget, reducer)
    end
  end

  defp reduce_list([], accumulator, _until_check, budget, _reducer) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, accumulator}
  end

  defp reduce_list([entry | entries], accumulator, until_check, budget, reducer) do
    case reducer.(entry, accumulator) do
      {:ok, accumulator} ->
        reduce_list(entries, accumulator, until_check - 1, budget, reducer)

      {:error, _reason} = error ->
        error
    end
  end

  defp reduce_entry(entry, {:ok, accumulator, 0}, budget, reducer) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> reduce_entry(entry, {:ok, accumulator, @deadline_check_interval}, budget, reducer)
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp reduce_entry(entry, {:ok, accumulator, until_check}, _budget, reducer) do
    case reducer.(entry, accumulator) do
      {:ok, accumulator} -> {:cont, {:ok, accumulator, until_check - 1}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp finish({:ok, accumulator, _until_check}, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, accumulator}
  end

  defp finish({:error, _reason} = error, _budget), do: error
end
