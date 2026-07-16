defmodule FerricStore.SDK.KV.Response do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.KV.{ResponseValue, ScalarResponseValue, SortedSetResponse}

  @type operation :: atom()
  @type result :: {:ok, term()} | {:error, term()}

  @spec set(result(), boolean(), boolean()) :: result()
  def set(result, true, _nx), do: validate(result, :set, &ResponseValue.binary_or_nil/1)
  def set(result, false, true), do: validate(result, :set, &ScalarResponseValue.boolean/1)
  def set(result, false, false), do: validate(result, :set, &ScalarResponseValue.set/1)

  @spec ok(result(), operation()) :: result()
  def ok(result, operation), do: validate(result, operation, &ScalarResponseValue.ok/1)

  @spec boolean_or_nil(result(), operation()) :: result()
  def boolean_or_nil(result, operation),
    do: validate(result, operation, &ScalarResponseValue.boolean_or_nil/1)

  @spec boolean(result(), operation()) :: result()
  def boolean(result, operation), do: validate(result, operation, &ScalarResponseValue.boolean/1)

  @spec one(result(), operation()) :: result()
  def one(result, operation), do: validate(result, operation, &ScalarResponseValue.one/1)

  @spec non_negative_integer(result(), operation()) :: result()
  def non_negative_integer(result, operation),
    do: validate(result, operation, &ScalarResponseValue.non_negative_integer/1)

  @spec bounded_count(result(), operation(), non_neg_integer()) :: result()
  def bounded_count({:ok, value}, _operation, maximum)
      when is_integer(value) and value >= 0 and value <= maximum,
      do: {:ok, value}

  def bounded_count({:ok, value}, operation, maximum)
      when is_integer(value) and value >= 0,
      do: invalid(operation, :count_exceeds_input, %{value: value, limit: maximum})

  def bounded_count({:ok, _value}, operation, _maximum),
    do: invalid(operation, :expected_non_negative_integer)

  def bounded_count({:error, _reason} = error, _operation, _maximum), do: error

  @spec list(result(), operation()) :: result()
  def list(result, operation), do: validate(result, operation, &ResponseValue.binary_list/1)

  def list(result, operation, %DeadlineBudget{} = budget),
    do: validate(result, operation, &ResponseValue.binary_list(&1, budget))

  @spec binary_or_nil(result(), operation()) :: result()
  def binary_or_nil(result, operation),
    do: validate(result, operation, &ResponseValue.binary_or_nil/1)

  @spec zrange(result(), boolean()) :: result()
  def zrange(result, withscores) when is_boolean(withscores),
    do: validate(result, :zrange, &SortedSetResponse.zrange(&1, withscores))

  def zrange(result, withscores, %DeadlineBudget{} = budget) when is_boolean(withscores),
    do: validate(result, :zrange, &SortedSetResponse.zrange(&1, withscores, budget))

  @spec exact_list(result(), operation(), non_neg_integer()) :: result()
  def exact_list(result, operation, expected) when is_integer(expected) and expected >= 0 do
    validate(result, operation, &ResponseValue.exact_binary_or_nil_list(&1, expected))
  end

  def exact_list(result, operation, expected, %DeadlineBudget{} = budget)
      when is_integer(expected) and expected >= 0 do
    validate(result, operation, &ResponseValue.exact_binary_or_nil_list(&1, expected, budget))
  end

  @spec map(result(), operation()) :: result()
  def map(result, operation), do: validate(result, operation, &ResponseValue.binary_map/1)

  def map(result, operation, %DeadlineBudget{} = budget),
    do: validate(result, operation, &ResponseValue.binary_map(&1, budget))

  @spec pop(result(), operation(), pos_integer()) :: result()
  def pop(result, operation, 1), do: validate(result, operation, &ScalarResponseValue.pop/1)

  def pop(result, operation, count) when is_integer(count) and count > 1,
    do: validate(result, operation, &list_pop_value(&1, count))

  def pop(result, operation, count, %DeadlineBudget{} = budget)
      when is_integer(count) and count > 0,
      do: validate(result, operation, &pop_value(&1, count, budget))

  @spec score(result(), operation()) :: result()
  def score(result, operation), do: validate(result, operation, &SortedSetResponse.score/1)

  defp validate({:ok, value}, operation, validator) do
    case validator.(value) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, :timeout} = error -> error
      {:error, reason} -> invalid(operation, reason)
    end
  end

  defp validate({:error, _reason} = error, _operation, _validator), do: error

  defp list_pop_value(nil, _count), do: {:ok, nil}

  defp list_pop_value(value, count),
    do: ResponseValue.bounded_nonempty_binary_list(value, count)

  defp pop_value(value, 1, _budget), do: ScalarResponseValue.pop(value)
  defp pop_value(nil, _count, _budget), do: {:ok, nil}

  defp pop_value(value, count, budget),
    do: ResponseValue.bounded_nonempty_binary_list(value, count, budget)

  defp invalid(operation, reason, details \\ %{}),
    do:
      {:error,
       {:invalid_kv_response, Map.merge(%{operation: operation, reason: reason}, details)}}
end
