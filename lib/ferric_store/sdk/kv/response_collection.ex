defmodule FerricStore.SDK.KV.ResponseCollection do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}

  @max_items RequestLimits.max_batch_items()
  @deadline_check_interval 256

  def binary_list(value, budget) when is_list(value) do
    with :ok <- validate_binaries(value, :expected_binary_list, @max_items, 0, 0, budget),
         do: {:ok, value}
  end

  def binary_list(_value, _budget), do: {:error, :expected_binary_list}

  def bounded_nonempty_binary_list([], _maximum, _budget),
    do: {:error, :expected_nonempty_binary_list}

  def bounded_nonempty_binary_list(value, maximum, budget)
      when is_list(value) and is_integer(maximum) and maximum > 0 do
    maximum = min(maximum, @max_items)

    with :ok <- validate_binaries(value, :expected_nonempty_binary_list, maximum, 0, 0, budget),
         do: {:ok, value}
  end

  def bounded_nonempty_binary_list(_value, _maximum, _budget),
    do: {:error, :expected_nonempty_binary_list}

  def binary_or_nil_list(value, budget) when is_list(value) do
    with :ok <-
           validate_binary_or_nil(
             value,
             :expected_binary_or_nil_list,
             @max_items,
             0,
             0,
             budget
           ),
         do: {:ok, value}
  end

  def binary_or_nil_list(_value, _budget), do: {:error, :expected_binary_or_nil_list}

  def exact_binary_or_nil_list(value, expected, budget)
      when is_list(value) and is_integer(expected) and expected >= 0 do
    with :ok <- validate_exact(value, expected, 0, 0, budget), do: {:ok, value}
  end

  def exact_binary_or_nil_list(_value, _expected, _budget),
    do: {:error, :expected_binary_or_nil_list}

  def binary_map(value, _budget) when is_map(value) and map_size(value) > @max_items,
    do: {:error, :collection_too_large}

  def binary_map(value, budget) when is_map(value) do
    result =
      Enum.reduce_while(value, {:ok, 0}, fn entry, state ->
        validate_map_entry(entry, state, budget)
      end)

    with :ok <- finish_map(result, budget), do: {:ok, value}
  end

  def binary_map(_value, _budget), do: {:error, :expected_binary_map}

  defp validate_binaries([], _error, _maximum, _count, _check, budget),
    do: ensure_active(budget)

  defp validate_binaries([_item | _items], _error, maximum, maximum, _check, budget) do
    with :ok <- ensure_active(budget), do: {:error, :too_many_items}
  end

  defp validate_binaries(items, error, maximum, count, 0, budget) do
    with :ok <- ensure_active(budget) do
      validate_binaries(items, error, maximum, count, @deadline_check_interval, budget)
    end
  end

  defp validate_binaries([item | items], error, maximum, count, check, budget)
       when is_binary(item),
       do: validate_binaries(items, error, maximum, count + 1, check - 1, budget)

  defp validate_binaries(_invalid, error, _maximum, _count, _check, _budget),
    do: {:error, error}

  defp validate_binary_or_nil([], _error, _maximum, _count, _check, budget),
    do: ensure_active(budget)

  defp validate_binary_or_nil([_item | _items], _error, maximum, maximum, _check, budget) do
    with :ok <- ensure_active(budget), do: {:error, :too_many_items}
  end

  defp validate_binary_or_nil(items, error, maximum, count, 0, budget) do
    with :ok <- ensure_active(budget) do
      validate_binary_or_nil(items, error, maximum, count, @deadline_check_interval, budget)
    end
  end

  defp validate_binary_or_nil([item | items], error, maximum, count, check, budget)
       when is_binary(item) or is_nil(item),
       do: validate_binary_or_nil(items, error, maximum, count + 1, check - 1, budget)

  defp validate_binary_or_nil(_invalid, error, _maximum, _count, _check, _budget),
    do: {:error, error}

  defp validate_exact([], expected, expected, _check, budget), do: ensure_active(budget)

  defp validate_exact([], _expected, _count, _check, _budget),
    do: {:error, :unexpected_cardinality}

  defp validate_exact([_item | _items], expected, expected, _check, _budget),
    do: {:error, :unexpected_cardinality}

  defp validate_exact(items, expected, count, 0, budget) do
    with :ok <- ensure_active(budget),
         do: validate_exact(items, expected, count, @deadline_check_interval, budget)
  end

  defp validate_exact([item | items], expected, count, check, budget)
       when is_binary(item) or is_nil(item) do
    validate_exact(items, expected, count + 1, check - 1, budget)
  end

  defp validate_exact(_value, _expected, _count, _check, _budget),
    do: {:error, :expected_binary_or_nil_list}

  defp validate_map_entry(entry, {:ok, 0}, budget) do
    case ensure_active(budget) do
      :ok -> validate_map_entry(entry, {:ok, @deadline_check_interval}, budget)
      {:error, :timeout} = error -> {:halt, error}
    end
  end

  defp validate_map_entry({key, value}, {:ok, check}, _budget)
       when is_binary(key) and is_binary(value),
       do: {:cont, {:ok, check - 1}}

  defp validate_map_entry(_entry, _state, _budget),
    do: {:halt, {:error, :expected_binary_map}}

  defp finish_map({:ok, _check}, budget), do: ensure_active(budget)

  defp finish_map({:error, _reason} = error, _budget), do: error

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
