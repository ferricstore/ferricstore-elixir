defmodule FerricStore.Flow.Options.ClaimCollectionValidator do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.Options.CollectionScan

  @max_filter_items 64

  @spec validate(atom(), keyword(), DeadlineBudget.t() | nil) :: :ok | {:error, term()}
  def validate(:claim_due, opts, budget) do
    with {:ok, state_count} <- validate_states(opts, budget),
         {:ok, partition_count} <- validate_partitions(opts, budget),
         do: validate_footprint(state_count, partition_count)
  end

  def validate(_operation, _opts, _budget), do: :ok

  defp validate_states(opts, budget) do
    case Keyword.fetch(opts, :states) do
      :error -> {:ok, 1}
      {:ok, nil} -> {:ok, 1}
      {:ok, []} -> invalid(:states, :expected_nonempty_state_list)
      {:ok, states} -> validate_state_list(states, budget)
    end
  end

  defp validate_state_list(states, budget) do
    case CollectionScan.validate(states, @max_filter_items, &state?/1, budget) do
      {:ok, _count} -> normalized_state_count(states)
      {:error, :too_large} -> invalid(:states, {:maximum_items, @max_filter_items})
      {:error, :timeout} = error -> error
      {:error, _reason} -> invalid(:states, :expected_state_list)
    end
  end

  defp normalized_state_count(states) do
    {any?, names} =
      Enum.reduce(states, {false, MapSet.new()}, fn state, {any?, names} ->
        if any_state?(state), do: {true, names}, else: {any?, MapSet.put(names, state)}
      end)

    cond do
      any? and MapSet.size(names) > 0 -> invalid(:states, :expected_state_list)
      any? -> {:ok, 1}
      true -> {:ok, max(MapSet.size(names), 1)}
    end
  end

  defp validate_partitions(opts, budget) do
    case Keyword.fetch(opts, :partition_keys) do
      :error -> {:ok, 1}
      {:ok, []} -> invalid(:partition_keys, :expected_nonempty_partition_key_list)
      {:ok, partitions} -> validate_partition_list(partitions, budget)
    end
  end

  defp validate_partition_list(partitions, budget) do
    case CollectionScan.validate(partitions, @max_filter_items, &nonempty_binary?/1, budget) do
      {:ok, _count} -> {:ok, max(partitions |> MapSet.new() |> MapSet.size(), 1)}
      {:error, :too_large} -> invalid(:partition_keys, {:maximum_items, @max_filter_items})
      {:error, :timeout} = error -> error
      {:error, _reason} -> invalid(:partition_keys, :expected_nonempty_partition_key_list)
    end
  end

  defp validate_footprint(states, partitions) when states <= div(@max_filter_items, partitions),
    do: :ok

  defp validate_footprint(_states, _partitions),
    do: invalid(:filters, {:maximum_filter_footprint, @max_filter_items})

  defp state?(value), do: any_state?(value) or nonempty_binary?(value)
  defp any_state?(:any), do: true
  defp any_state?(<<a, n, y>>) when a in [?a, ?A] and n in [?n, ?N] and y in [?y, ?Y], do: true
  defp any_state?(_value), do: false
  defp nonempty_binary?(value), do: is_binary(value) and value != ""

  defp invalid(option, expectation),
    do: {:error, {:invalid_flow_option, :claim_due, option, expectation}}
end
