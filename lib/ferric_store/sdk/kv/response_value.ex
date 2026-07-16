defmodule FerricStore.SDK.KV.ResponseValue do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.KV.ResponseCollection

  @spec binary_or_nil(term()) :: {:ok, binary() | nil} | {:error, atom()}
  def binary_or_nil(value) when is_binary(value) or is_nil(value), do: {:ok, value}
  def binary_or_nil(_value), do: {:error, :expected_binary_or_nil}

  @spec binary_or_nil?(term()) :: boolean()
  def binary_or_nil?(value), do: is_binary(value) or is_nil(value)

  @spec binary_list(term()) :: {:ok, [binary()]} | {:error, atom()}
  def binary_list(value), do: ResponseCollection.binary_list(value, nil)

  def binary_list(value, %DeadlineBudget{} = budget),
    do: ResponseCollection.binary_list(value, budget)

  @spec bounded_nonempty_binary_list(term(), pos_integer()) ::
          {:ok, [binary()]} | {:error, atom()}
  def bounded_nonempty_binary_list(value, maximum),
    do: ResponseCollection.bounded_nonempty_binary_list(value, maximum, nil)

  def bounded_nonempty_binary_list(value, maximum, %DeadlineBudget{} = budget),
    do: ResponseCollection.bounded_nonempty_binary_list(value, maximum, budget)

  @spec binary_or_nil_list(term()) :: {:ok, [binary() | nil]} | {:error, atom()}
  def binary_or_nil_list(value), do: ResponseCollection.binary_or_nil_list(value, nil)

  def binary_or_nil_list(value, %DeadlineBudget{} = budget),
    do: ResponseCollection.binary_or_nil_list(value, budget)

  @spec exact_binary_or_nil_list(term(), non_neg_integer()) ::
          {:ok, [binary() | nil]} | {:error, atom()}
  def exact_binary_or_nil_list(value, expected),
    do: ResponseCollection.exact_binary_or_nil_list(value, expected, nil)

  def exact_binary_or_nil_list(value, expected, %DeadlineBudget{} = budget),
    do: ResponseCollection.exact_binary_or_nil_list(value, expected, budget)

  @spec binary_map(term()) :: {:ok, %{binary() => binary()}} | {:error, atom()}
  def binary_map(value), do: ResponseCollection.binary_map(value, nil)

  def binary_map(value, %DeadlineBudget{} = budget),
    do: ResponseCollection.binary_map(value, budget)
end
