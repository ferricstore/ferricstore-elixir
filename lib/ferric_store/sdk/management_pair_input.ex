defmodule FerricStore.SDK.ManagementPairInput do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.SDK.{ManagementInputError, ManagementPairNormalizer}

  @max_pairs div(RequestLimits.max_command_items() - 2, 2)
  @deadline_check_interval 256

  @spec args(term(), atom(), atom(), DeadlineBudget.t()) ::
          {:ok, list()} | {:error, :timeout | term()}
  def args(nil, _operation, _field, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, []}
  end

  def args(%{} = pairs, operation, field, %DeadlineBudget{} = budget) do
    if map_size(pairs) > @max_pairs do
      with :ok <- DeadlineBudget.ensure_active(budget),
           do: ManagementInputError.too_many(operation, field, @max_pairs, map_size(pairs))
    else
      map_args(pairs, operation, field, budget)
    end
  end

  def args(pairs, operation, field, %DeadlineBudget{} = budget) when is_list(pairs),
    do: list_args(pairs, operation, field, 0, MapSet.new(), [], 0, budget)

  def args(value, operation, field, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      ManagementInputError.invalid(operation, field, :expected_map_or_pair_list, %{value: value})
    end
  end

  defp map_args(pairs, operation, field, budget) do
    pairs
    |> Enum.reduce_while({:ok, 0, MapSet.new(), [], 0}, fn pair, state ->
      reduce_pair(pair, state, operation, field, budget)
    end)
    |> finish_map(budget)
  end

  defp reduce_pair(pair, {:ok, index, seen, args, 0}, operation, field, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok ->
        reduce_pair(
          pair,
          {:ok, index, seen, args, @deadline_check_interval},
          operation,
          field,
          budget
        )

      {:error, _reason} = error ->
        {:halt, error}
    end
  end

  defp reduce_pair(pair, {:ok, index, seen, args, until_check}, operation, field, _budget) do
    case ManagementPairNormalizer.normalize(pair, operation, field, index, seen, args) do
      {:ok, seen, args} -> {:cont, {:ok, index + 1, seen, args, until_check - 1}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp list_args(pairs, operation, field, index, seen, args, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      list_args(pairs, operation, field, index, seen, args, @deadline_check_interval, budget)
    end
  end

  defp list_args([], _operation, _field, _index, _seen, args, _until_check, budget),
    do: finish_args(args, budget)

  defp list_args([_pair | _pairs], operation, field, @max_pairs, _seen, _args, _check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: ManagementInputError.too_many(operation, field, @max_pairs, @max_pairs + 1)
  end

  defp list_args([pair | pairs], operation, field, index, seen, args, check, budget) do
    with {:ok, seen, args} <-
           ManagementPairNormalizer.normalize(pair, operation, field, index, seen, args) do
      list_args(pairs, operation, field, index + 1, seen, args, check - 1, budget)
    end
  end

  defp list_args(_tail, operation, field, index, _seen, _args, _check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: ManagementInputError.invalid(operation, field, :improper_list, %{index: index})
  end

  defp finish_map({:ok, _index, _seen, args, _check}, budget), do: finish_args(args, budget)
  defp finish_map({:error, _reason} = error, _budget), do: error

  defp finish_args(args, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         args = Enum.reverse(args),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, args}
  end
end
