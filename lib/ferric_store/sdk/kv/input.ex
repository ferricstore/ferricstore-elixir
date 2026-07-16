defmodule FerricStore.SDK.KV.Input do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}

  alias FerricStore.SDK.KV.{
    HashFieldsInput,
    MSetInput,
    RouteKeyInput,
    ScalarInput,
    SortedSetInput
  }

  @max_batch_items RequestLimits.max_batch_items()
  @deadline_check_interval 256

  @spec binary_list(term(), atom(), atom(), DeadlineBudget.t(), boolean()) ::
          {:ok, [binary()], non_neg_integer()}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_kv_input, map()}}
  def binary_list(value, operation, field, budget, allow_empty? \\ false)

  def binary_list(value, operation, field, %DeadlineBudget{} = budget, allow_empty?) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      do_binary_list(value, operation, field, budget, allow_empty?)
    end
  end

  defp do_binary_list([], operation, field, _budget, false),
    do: invalid_input(operation, field, :empty)

  defp do_binary_list(items, operation, field, budget, _allow_empty?) when is_list(items) do
    case validate_binary_items(items, operation, field, 0, 0, budget) do
      {:ok, count} -> {:ok, items, count}
      {:error, _reason} = error -> error
    end
  end

  defp do_binary_list(value, operation, field, _budget, _allow_empty?),
    do: invalid_input(operation, field, :expected_list, %{value: value})

  defdelegate route_key_list(value, operation, budget, allow_empty? \\ false),
    to: RouteKeyInput,
    as: :list

  @spec binary_or_list(term(), atom(), atom(), DeadlineBudget.t(), boolean()) ::
          {:ok, [binary()], non_neg_integer()}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_kv_input, map()}}
  def binary_or_list(value, operation, field, budget, allow_empty? \\ false)

  def binary_or_list(value, operation, field, %DeadlineBudget{} = budget, allow_empty?) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      do_binary_or_list(value, operation, field, budget, allow_empty?)
    end
  end

  defp do_binary_or_list(value, _operation, _field, _budget, _allow_empty?)
       when is_binary(value),
       do: {:ok, [value], 1}

  defp do_binary_or_list(value, operation, field, budget, allow_empty?) when is_list(value),
    do: do_binary_list(value, operation, field, budget, allow_empty?)

  defp do_binary_or_list(value, operation, field, _budget, _allow_empty?),
    do: invalid_input(operation, field, :expected_binary_or_list, %{value: value})

  defdelegate route_key_or_list(value, operation, budget, allow_empty? \\ false),
    to: RouteKeyInput,
    as: :one_or_list

  defdelegate hash_fields(fields, budget), to: HashFieldsInput, as: :validate

  defdelegate mset_pairs(pairs, budget), to: MSetInput, as: :pairs

  @spec binary(term(), atom(), atom()) ::
          {:ok, binary()} | {:error, {:invalid_kv_input, map()}}
  defdelegate binary(value, operation, field), to: ScalarInput

  @spec nonempty_binary(term(), atom(), atom()) ::
          {:ok, binary()} | {:error, {:invalid_kv_input, map()}}
  defdelegate nonempty_binary(value, operation, field), to: ScalarInput

  @spec integer(term(), atom(), atom()) ::
          {:ok, integer()} | {:error, {:invalid_kv_input, map()}}
  defdelegate integer(value, operation, field), to: ScalarInput

  @spec non_negative_integer(term(), atom(), atom()) ::
          {:ok, non_neg_integer()} | {:error, {:invalid_kv_input, map()}}
  defdelegate non_negative_integer(value, operation, field), to: ScalarInput

  @spec positive_integer(term(), atom(), atom()) ::
          {:ok, pos_integer()} | {:error, {:invalid_kv_input, map()}}
  defdelegate positive_integer(value, operation, field), to: ScalarInput

  @spec collection_count(term(), atom(), atom()) ::
          {:ok, pos_integer()}
          | {:error, {:batch_too_large, map()} | {:invalid_kv_input, map()}}
  def collection_count(value, operation, field) do
    case ScalarInput.positive_integer(value, operation, field) do
      {:ok, count} when count <= @max_batch_items -> {:ok, count}
      {:ok, count} -> batch_too_large(count)
      {:error, _reason} = error -> error
    end
  end

  @spec optional_boolean(term(), atom(), atom()) ::
          {:ok, boolean() | nil} | {:error, {:invalid_kv_input, map()}}
  defdelegate optional_boolean(value, operation, field), to: ScalarInput

  defdelegate zadd_items(items, budget), to: SortedSetInput

  defp validate_binary_items([], _operation, _field, count, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, count}
  end

  defp validate_binary_items(
         [_item | _items],
         _operation,
         _field,
         @max_batch_items,
         _until_check,
         budget
       ) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: batch_too_large(@max_batch_items + 1)
  end

  defp validate_binary_items(items, operation, field, index, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_binary_items(items, operation, field, index, @deadline_check_interval, budget)
    end
  end

  defp validate_binary_items([item | items], operation, field, index, until_check, budget)
       when is_binary(item) do
    validate_binary_items(items, operation, field, index + 1, until_check - 1, budget)
  end

  defp validate_binary_items(
         [item | _items],
         operation,
         field,
         index,
         _until_check,
         _budget
       ),
       do:
         invalid_input(operation, field, :expected_binary, %{
           index: index,
           value: item
         })

  defp validate_binary_items(
         _improper_tail,
         operation,
         field,
         index,
         _until_check,
         budget
       ) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid_input(operation, field, :improper_list, %{index: index})
  end

  defp batch_too_large(observed),
    do: {:error, {:batch_too_large, %{items: observed, limit: @max_batch_items}}}

  defp invalid_input(operation, field, reason, details \\ %{}) do
    {:error,
     {:invalid_kv_input,
      Map.merge(%{operation: operation, field: field, reason: reason}, details)}}
  end
end
