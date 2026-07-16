defmodule FerricStore.SDK.ManagementInput do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.{ManagementInputError, ManagementPairInput, ManagementRuleInput}

  @spec normalize_rules(term(), DeadlineBudget.t()) ::
          {:ok, [binary()]} | {:error, :timeout | term()}
  defdelegate normalize_rules(rules, budget), to: ManagementRuleInput, as: :normalize

  @spec nonempty_binary(term(), atom(), atom()) :: {:ok, binary()} | {:error, term()}
  def nonempty_binary(value, _operation, _field) when is_binary(value) and value != "",
    do: {:ok, value}

  def nonempty_binary(value, operation, field),
    do: ManagementInputError.invalid(operation, field, :expected_nonempty_binary, %{value: value})

  @spec pair_args(term(), atom(), atom(), DeadlineBudget.t()) ::
          {:ok, list()} | {:error, :timeout | term()}
  defdelegate pair_args(pairs, operation, field, budget), to: ManagementPairInput, as: :args
end
