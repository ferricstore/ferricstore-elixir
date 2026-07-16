defmodule FerricStore.SDK.KV.MSetInput do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits, RouteKey}
  alias FerricStore.SDK.KV.MSetPair

  @max_batch_items RequestLimits.max_batch_items()
  @max_route_key_bytes RouteKey.max_bytes()
  @deadline_check_interval 256

  @spec pairs(term(), DeadlineBudget.t()) ::
          {:ok, map() | list(), non_neg_integer()}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_mset_pairs, term()}}
  def pairs(pairs, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: validate(pairs, budget)
  end

  defp validate(pairs, _budget)
       when is_map(pairs) and map_size(pairs) > @max_batch_items,
       do: batch_too_large()

  defp validate(pairs, _budget) when is_map(pairs),
    do: {:ok, pairs, map_size(pairs)}

  defp validate(pairs, budget) when is_list(pairs) do
    case validate_list(pairs, 0, 0, budget) do
      {:ok, item_count} -> {:ok, pairs, item_count}
      {:error, _reason} = error -> error
    end
  end

  defp validate(pairs, _budget), do: {:error, {:invalid_mset_pairs, pairs}}

  defp validate_list([], count, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, count}
  end

  defp validate_list([_pair | _pairs], @max_batch_items, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: batch_too_large()
  end

  defp validate_list(pairs, index, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_list(pairs, index, @deadline_check_interval, budget)
    end
  end

  defp validate_list([{key, value} | pairs], index, until_check, budget)
       when is_binary(key) and is_binary(value) and byte_size(key) <= @max_route_key_bytes,
       do: validate_list(pairs, index + 1, until_check - 1, budget)

  defp validate_list([pair | pairs], index, until_check, budget) do
    case MSetPair.normalize(pair) do
      {:ok, {key, _value}} when byte_size(key) <= @max_route_key_bytes ->
        validate_list(pairs, index + 1, until_check - 1, budget)

      {:ok, {key, _value}} ->
        RouteKey.validate(key)

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_list(_improper_tail, _index, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: {:error, {:invalid_mset_pairs, :improper_list}}
  end

  defp batch_too_large,
    do: {:error, {:batch_too_large, %{items: @max_batch_items + 1, limit: @max_batch_items}}}
end
