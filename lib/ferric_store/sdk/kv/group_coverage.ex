defmodule FerricStore.SDK.KV.GroupCoverage do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @deadline_check_interval 256

  @enforce_keys [:expected, :seen]
  defstruct [:expected, :seen, count: 0]

  @type t :: %__MODULE__{
          expected: non_neg_integer(),
          seen: :array.array(boolean()),
          count: non_neg_integer()
        }

  @spec new(non_neg_integer()) :: t()
  def new(expected) when is_integer(expected) and expected >= 0 do
    %__MODULE__{
      expected: expected,
      seen: :array.new(expected, default: false, fixed: true)
    }
  end

  @spec add(t(), list()) :: {:ok, t(), non_neg_integer()} | {:error, map()}
  def add(%__MODULE__{}, []), do: {:error, %{reason: :empty_indexes}}

  def add(%__MODULE__{} = coverage, indexes) when is_list(indexes),
    do: add_indexes(indexes, coverage, 0, 0, nil)

  def add(%__MODULE__{}, _indexes), do: {:error, %{reason: :invalid_indexes}}

  @spec add(t(), list(), DeadlineBudget.t()) ::
          {:ok, t(), non_neg_integer()} | {:error, :timeout | map()}
  def add(%__MODULE__{}, [], %DeadlineBudget{}), do: {:error, %{reason: :empty_indexes}}

  def add(%__MODULE__{} = coverage, indexes, %DeadlineBudget{} = budget)
      when is_list(indexes),
      do: add_indexes(indexes, coverage, 0, 0, budget)

  def add(%__MODULE__{}, _indexes, %DeadlineBudget{}),
    do: {:error, %{reason: :invalid_indexes}}

  @spec complete(t()) :: :ok | {:error, map()}
  def complete(%__MODULE__{expected: expected, count: expected}), do: :ok

  def complete(%__MODULE__{expected: expected, count: count}) do
    {:error, %{reason: :incomplete_groups, expected_items: expected, actual_items: count}}
  end

  defp add_indexes([], coverage, group_count, _check, budget) do
    with :ok <- ensure_active(budget), do: {:ok, coverage, group_count}
  end

  defp add_indexes(indexes, coverage, group_count, 0, budget) do
    with :ok <- ensure_active(budget) do
      add_indexes(indexes, coverage, group_count, @deadline_check_interval, budget)
    end
  end

  defp add_indexes([index | indexes], coverage, group_count, check, budget) do
    cond do
      not is_integer(index) or index < 0 or index >= coverage.expected ->
        {:error,
         %{
           reason: :invalid_index,
           expected_range: {0, coverage.expected - 1}
         }}

      :array.get(index, coverage.seen) ->
        {:error, %{reason: :duplicate_index, index: index}}

      true ->
        coverage = %{
          coverage
          | seen: :array.set(index, true, coverage.seen),
            count: coverage.count + 1
        }

        add_indexes(indexes, coverage, group_count + 1, check - 1, budget)
    end
  end

  defp add_indexes(_improper_tail, _coverage, _group_count, _check, _budget),
    do: {:error, %{reason: :improper_indexes}}

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
