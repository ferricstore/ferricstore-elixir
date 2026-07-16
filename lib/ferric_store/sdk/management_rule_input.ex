defmodule FerricStore.SDK.ManagementRuleInput do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.SDK.ManagementInputError

  @max_rules RequestLimits.max_command_items() - 2
  @deadline_check_interval 256

  @spec normalize(term(), DeadlineBudget.t()) ::
          {:ok, [binary()]} | {:error, :timeout | term()}
  def normalize(rules, %DeadlineBudget{} = budget) when is_list(rules) do
    normalize_list(rules, 0, [], 0, budget)
  end

  def normalize(rule, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, rule} <- normalize_rule(rule, 0),
         :ok <- DeadlineBudget.ensure_active(budget) do
      {:ok, [rule]}
    end
  end

  defp normalize_list(rules, index, normalized, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      normalize_list(rules, index, normalized, @deadline_check_interval, budget)
    end
  end

  defp normalize_list([], _index, normalized, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         normalized = Enum.reverse(normalized),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, normalized}
  end

  defp normalize_list([_rule | _rules], @max_rules, _normalized, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: ManagementInputError.too_many(:set_user, :rules, @max_rules, @max_rules + 1)
  end

  defp normalize_list([rule | rules], index, normalized, until_check, budget) do
    with {:ok, rule} <- normalize_rule(rule, index) do
      normalize_list(rules, index + 1, [rule | normalized], until_check - 1, budget)
    end
  end

  defp normalize_list(_improper_tail, index, _normalized, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      ManagementInputError.invalid(:set_user, :rules, :improper_list, %{index: index})
    end
  end

  defp normalize_rule(rule, _index) when is_binary(rule) and rule != "", do: {:ok, rule}

  defp normalize_rule(rule, index) when is_atom(rule) and rule not in [nil, true, false] do
    case Atom.to_string(rule) do
      "" -> invalid_rule(rule, index)
      value -> {:ok, value}
    end
  end

  defp normalize_rule(rule, index), do: invalid_rule(rule, index)

  defp invalid_rule(rule, index),
    do:
      ManagementInputError.invalid(:set_user, :rules, :invalid_rule, %{
        index: index,
        value: rule
      })
end
