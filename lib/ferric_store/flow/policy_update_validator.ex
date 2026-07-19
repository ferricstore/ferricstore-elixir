defmodule FerricStore.Flow.PolicyUpdateValidator do
  @moduledoc false

  @max_generation 9_007_199_254_740_991

  @spec validate(map()) :: :ok | {:error, {:invalid_policy_option, binary()}}
  def validate(options) when is_map(options) do
    with :ok <- validate_replace(Map.fetch(options, "replace")) do
      validate_expected_generation(Map.fetch(options, "expected_generation"))
    end
  end

  @spec max_generation() :: pos_integer()
  def max_generation, do: @max_generation

  defp validate_replace(:error), do: :ok
  defp validate_replace({:ok, value}) when is_boolean(value), do: :ok
  defp validate_replace({:ok, _value}), do: invalid("replace")

  defp validate_expected_generation(:error), do: :ok

  defp validate_expected_generation({:ok, generation})
       when is_integer(generation) and generation >= 0 and generation <= @max_generation,
       do: :ok

  defp validate_expected_generation({:ok, _value}), do: invalid("expected_generation")

  defp invalid(field), do: {:error, {:invalid_policy_option, field}}
end
