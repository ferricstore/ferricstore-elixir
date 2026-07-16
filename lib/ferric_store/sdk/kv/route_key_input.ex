defmodule FerricStore.SDK.KV.RouteKeyInput do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits, RouteKey}

  @max_batch_items RequestLimits.max_batch_items()
  @max_route_key_bytes RouteKey.max_bytes()
  @deadline_check_interval 256

  @spec list(term(), atom(), DeadlineBudget.t(), boolean()) ::
          {:ok, [binary()], non_neg_integer()} | {:error, term()}
  def list(value, operation, %DeadlineBudget{} = budget, allow_empty? \\ false) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_container(value, operation, budget, allow_empty?)
    end
  end

  @spec one_or_list(term(), atom(), DeadlineBudget.t(), boolean()) ::
          {:ok, [binary()], non_neg_integer()} | {:error, term()}
  def one_or_list(value, operation, %DeadlineBudget{} = budget, allow_empty? \\ false) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_one_or_list(value, operation, budget, allow_empty?)
    end
  end

  defp validate_one_or_list(value, _operation, _budget, _allow_empty?) when is_binary(value) do
    with {:ok, value} <- RouteKey.validate(value), do: {:ok, [value], 1}
  end

  defp validate_one_or_list(value, operation, budget, allow_empty?) when is_list(value),
    do: validate_container(value, operation, budget, allow_empty?)

  defp validate_one_or_list(value, operation, _budget, _allow_empty?),
    do: invalid(operation, :expected_binary_or_list, %{value: value})

  defp validate_container([], operation, _budget, false), do: invalid(operation, :empty)

  defp validate_container(keys, operation, budget, _allow_empty?) when is_list(keys) do
    case validate_items(keys, operation, 0, 0, budget) do
      {:ok, count} -> {:ok, keys, count}
      {:error, _reason} = error -> error
    end
  end

  defp validate_container(value, operation, _budget, _allow_empty?),
    do: invalid(operation, :expected_list, %{value: value})

  defp validate_items([], _operation, count, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, count}
  end

  defp validate_items([_key | _keys], _operation, @max_batch_items, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: batch_too_large()
  end

  defp validate_items(keys, operation, index, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_items(keys, operation, index, @deadline_check_interval, budget)
    end
  end

  defp validate_items([key | keys], operation, index, until_check, budget)
       when is_binary(key) and byte_size(key) <= @max_route_key_bytes,
       do: validate_items(keys, operation, index + 1, until_check - 1, budget)

  defp validate_items([key | _keys], _operation, _index, _until_check, _budget)
       when is_binary(key),
       do: RouteKey.validate(key)

  defp validate_items([key | _keys], operation, index, _until_check, _budget),
    do: invalid(operation, :expected_binary, %{index: index, value: key})

  defp validate_items(_improper_tail, operation, index, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid(operation, :improper_list, %{index: index})
  end

  defp batch_too_large do
    {:error, {:batch_too_large, %{items: @max_batch_items + 1, limit: @max_batch_items}}}
  end

  defp invalid(operation, reason, details \\ %{}) do
    {:error,
     {:invalid_kv_input,
      Map.merge(%{operation: operation, field: :keys, reason: reason}, details)}}
  end
end
