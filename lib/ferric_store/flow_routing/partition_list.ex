defmodule FerricStore.FlowRouting.PartitionList do
  @moduledoc false

  alias FerricStore.{BoundedList, DeadlineBudget, RequestLimits}
  alias FerricStore.FlowRouting.PartitionScan

  @max_partitions RequestLimits.max_batch_items()

  @type resolution ::
          :none
          | {:ok, binary()}
          | {:error, :timeout | {:invalid_route_key, term()} | {:batch_too_large, map()}}

  @spec resolve(term(), (binary() -> resolution())) :: resolution()
  def resolve(partitions, resolver) when is_function(resolver, 1) do
    with :ok <- validate_cardinality(partitions, nil) do
      PartitionScan.run(partitions, resolver, nil)
    end
  end

  @spec resolve(term(), (binary() -> resolution()), DeadlineBudget.t()) :: resolution()
  def resolve(partitions, resolver, %DeadlineBudget{} = budget) when is_function(resolver, 1) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         :ok <- validate_cardinality(partitions, budget),
         resolution <- PartitionScan.run(partitions, resolver, budget),
         :ok <- finish_deadline(resolution, budget) do
      resolution
    end
  end

  defp finish_deadline({:error, :timeout}, _budget), do: :ok
  defp finish_deadline(_resolution, budget), do: DeadlineBudget.ensure_active(budget)

  defp validate_cardinality(partitions, nil) when is_list(partitions) do
    partitions |> BoundedList.count(@max_partitions) |> cardinality_result(partitions)
  end

  defp validate_cardinality(partitions, %DeadlineBudget{} = budget) when is_list(partitions) do
    partitions
    |> BoundedList.count(@max_partitions, budget)
    |> cardinality_result(partitions)
  end

  defp validate_cardinality(partitions, _budget), do: invalid(partitions)

  defp cardinality_result({:ok, 0}, partitions), do: invalid(partitions)
  defp cardinality_result({:ok, _count}, _partitions), do: :ok

  defp cardinality_result({:error, {:limit_exceeded, _observed}}, _partitions),
    do: batch_too_large()

  defp cardinality_result({:error, :improper_list}, partitions), do: invalid(partitions)
  defp cardinality_result({:error, :timeout} = error, _partitions), do: error

  defp invalid(partitions), do: {:error, {:invalid_route_key, partitions}}

  defp batch_too_large,
    do: {:error, {:batch_too_large, %{items: @max_partitions + 1, limit: @max_partitions}}}
end
