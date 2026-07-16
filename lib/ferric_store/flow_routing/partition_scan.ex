defmodule FerricStore.FlowRouting.PartitionScan do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RouteKey, RoutingSlot}

  @deadline_check_interval 256

  @spec run(list(), (binary() -> term()), DeadlineBudget.t() | nil) :: term()
  def run(partitions, resolver, budget) when is_list(partitions) and is_function(resolver, 1) do
    scan(partitions, partitions, resolver, nil, nil, budget, 0)
  end

  defp scan([], _original, _resolver, resolution, _slot, _budget, _until_check),
    do: resolution

  defp scan(
         [partition | partitions],
         original,
         resolver,
         resolution,
         slot,
         budget,
         until_check
       )
       when is_binary(partition) and partition != "" do
    with {:ok, next_check} <- advance_deadline(budget, until_check) do
      {resolution, slot} = update_resolution(resolution, slot, partition, resolver)
      scan(partitions, original, resolver, resolution, slot, budget, next_check)
    end
  end

  defp scan(_invalid_tail, original, _resolver, _resolution, _slot, _budget, _check),
    do: invalid(original)

  defp update_resolution(nil, _slot, partition, resolver) do
    case resolver.(partition) do
      {:ok, route_key} -> {{:ok, route_key}, RoutingSlot.for_key(route_key)}
      resolution -> {resolution, nil}
    end
  end

  defp update_resolution({:ok, first}, slot, partition, resolver) do
    case resolver.(partition) do
      {:ok, route_key} ->
        if RoutingSlot.for_key(route_key) == slot,
          do: {{:ok, first}, slot},
          else: {:none, nil}

      resolution ->
        {resolution, nil}
    end
  end

  defp update_resolution(:none, slot, partition, _resolver) do
    case RouteKey.validate(partition) do
      {:ok, _partition} -> {:none, slot}
      {:error, _reason} = error -> {error, nil}
    end
  end

  defp update_resolution(resolution, slot, _partition, _resolver), do: {resolution, slot}

  defp advance_deadline(nil, _until_check), do: {:ok, 0}

  defp advance_deadline(budget, 0) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, @deadline_check_interval - 1}
  end

  defp advance_deadline(_budget, until_check), do: {:ok, until_check - 1}

  defp invalid(partitions), do: {:error, {:invalid_route_key, partitions}}
end
