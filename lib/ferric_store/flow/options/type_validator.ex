defmodule FerricStore.Flow.Options.TypeValidator do
  @moduledoc false

  alias FerricStore.Flow.Options.TypeBooleanRules

  @boolean_options TypeBooleanRules.options()

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
