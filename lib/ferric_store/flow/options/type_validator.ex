defmodule FerricStore.Flow.Options.TypeValidator do
  @moduledoc false

  @boolean_options %{
    consistent_projection: [:history, :list, :search],
    full: [:get],
    idempotent: [:create, :create_many],
    include_attributes: [:claim_due],
    include_cold: [:history, :list],
    include_record: [:claim_due],
    include_state: [:claim_due],
    independent: [:complete_many, :create_many],
    local_cache: [:value_put],
    override: [:value_put],
    payload: [:claim_due, :get],
    reclaim_expired: [:claim_due],
    return_ok_on_success: [:complete_many, :create_many],
    rev: [:history, :list, :search],
    terminal_only: [:search],
    values: [:history]
  }

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    Enum.reduce_while(@boolean_options, :ok, fn {option, operations}, :ok ->
      validate_boolean(operation, opts, option, operations)
    end)
  end

  defp validate_boolean(operation, opts, option, operations) do
    if operation in operations,
      do: boolean_result(Keyword.fetch(opts, option), operation, option),
      else: {:cont, :ok}
  end

  defp boolean_result(:error, _operation, _option), do: {:cont, :ok}

  defp boolean_result({:ok, value}, _operation, _option)
       when is_nil(value) or is_boolean(value),
       do: {:cont, :ok}

  defp boolean_result({:ok, _value}, operation, option),
    do: {:halt, invalid(operation, option, :expected_boolean)}

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
