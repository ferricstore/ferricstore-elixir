defmodule FerricStore.SDK.KV.HashFieldsInput do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}

  @max_fields RequestLimits.max_batch_items()
  @deadline_check_interval 256

  @spec validate(term(), DeadlineBudget.t()) ::
          {:ok, %{binary() => binary()}, pos_integer()}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_kv_input, map()}}
  def validate(fields, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate_fields(fields, budget)
    end
  end

  defp validate_fields(fields, _budget) when is_map(fields) and map_size(fields) == 0,
    do: invalid(:empty)

  defp validate_fields(fields, _budget)
       when is_map(fields) and map_size(fields) > @max_fields,
       do: {:error, {:batch_too_large, %{items: @max_fields + 1, limit: @max_fields}}}

  defp validate_fields(fields, budget) when is_map(fields) do
    case validate_entries(fields, budget) do
      :ok -> {:ok, fields, map_size(fields)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_fields(_fields, _budget), do: invalid(:expected_map)

  defp validate_entries(fields, budget) do
    fields
    |> Enum.reduce_while({:ok, 0}, fn entry, state -> validate_entry(entry, state, budget) end)
    |> finish(budget)
  end

  defp validate_entry(entry, {:ok, 0}, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> validate_entry(entry, {:ok, @deadline_check_interval}, budget)
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp validate_entry({field, _value}, {:ok, _until_check}, _budget)
       when not is_binary(field),
       do: {:halt, invalid(:expected_binary_field)}

  defp validate_entry({_field, value}, {:ok, _until_check}, _budget)
       when not is_binary(value),
       do: {:halt, invalid(:expected_binary_value)}

  defp validate_entry({_field, _value}, {:ok, until_check}, _budget),
    do: {:cont, {:ok, until_check - 1}}

  defp finish({:ok, _until_check}, budget), do: DeadlineBudget.ensure_active(budget)
  defp finish({:error, _reason} = error, _budget), do: error

  defp invalid(reason),
    do: {:error, {:invalid_kv_input, %{operation: :hset, field: :fields, reason: reason}}}
end
