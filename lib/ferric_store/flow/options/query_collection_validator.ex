defmodule FerricStore.Flow.Options.QueryCollectionValidator do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.Flow.Options.CollectionScan

  @max_items RequestLimits.max_batch_items()

  @spec validate(atom(), keyword(), DeadlineBudget.t() | nil) :: :ok | {:error, term()}
  def validate(:signal, opts, budget), do: validate_states(opts, :if_state, budget)

  def validate(operation, opts, budget) when operation in [:claim_due, :get],
    do: validate_values(operation, opts, budget)

  def validate(_operation, _opts, _budget), do: :ok

  defp validate_states(opts, option, budget) do
    case Keyword.fetch(opts, option) do
      :error -> :ok
      {:ok, value} when is_binary(value) and value != "" -> :ok
      {:ok, states} -> list_result(states, :signal, option, :expected_state_or_state_list, budget)
    end
  end

  defp validate_values(operation, opts, budget) do
    case Keyword.fetch(opts, :values) do
      :error ->
        :ok

      {:ok, value} when is_boolean(value) ->
        :ok

      {:ok, value} when is_binary(value) and value != "" ->
        :ok

      {:ok, names} ->
        list_result(names, operation, :values, :expected_value_name_selection, budget)
    end
  end

  defp list_result(values, operation, option, expectation, budget) do
    case CollectionScan.validate(values, @max_items, &nonempty_binary?/1, budget) do
      {:ok, _count} -> :ok
      {:error, :timeout} = error -> error
      {:error, :too_large} -> invalid(operation, option, {:maximum_items, @max_items})
      {:error, _reason} -> invalid(operation, option, expectation)
    end
  end

  defp nonempty_binary?(value), do: is_binary(value) and value != ""

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
