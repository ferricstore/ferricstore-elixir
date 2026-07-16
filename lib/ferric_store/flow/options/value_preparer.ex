defmodule FerricStore.Flow.Options.ValuePreparer do
  @moduledoc false

  alias FerricStore.Codec.Raw
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.CodecRuntime
  alias FerricStore.Flow.Options.{PreparedMap, PreparedValues}

  @deadline_check_interval 32

  @spec prepare(keyword(), DeadlineBudget.t()) :: {:ok, keyword()} | {:error, :timeout}
  def prepare(opts, %DeadlineBudget{} = budget) do
    case Keyword.fetch(opts, :values) do
      {:ok, %PreparedMap{value: values}} -> encode_values(opts, values, budget)
      _missing_or_nil -> {:ok, opts}
    end
  end

  defp encode_values(opts, values, budget) do
    codec = Keyword.get(opts, :codec, Raw)

    with {:ok, raw_binary_values?} <- raw_binary_values?(values, codec, budget),
         do: prepare_values(raw_binary_values?, opts, values, codec, budget)
  end

  defp prepare_values(true, opts, values, _codec, _budget),
    do: {:ok, Keyword.replace!(opts, :values, PreparedValues.new(values))}

  defp prepare_values(false, opts, values, codec, budget) do
    case CodecRuntime.run(budget, codec, fn -> encode_entries(values, codec, budget) end) do
      {:ok, {:ok, encoded, _until_check}} ->
        {:ok, Keyword.replace!(opts, :values, PreparedValues.new(encoded))}

      {:ok, {:error, :timeout} = error} ->
        error

      {:error, :timeout} = error ->
        error
    end
  end

  defp raw_binary_values?(_values, codec, budget) when codec != Raw do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> {:ok, false}
      {:error, :timeout} = error -> error
    end
  end

  defp raw_binary_values?(values, Raw, budget) do
    values
    |> Enum.reduce_while({:ok, @deadline_check_interval}, fn {_key, value}, {:ok, until_check} ->
      raw_binary_value(value, until_check, budget)
    end)
    |> finish_raw_binary_scan(budget)
  end

  defp raw_binary_value(value, 0, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> raw_binary_value(value, @deadline_check_interval, budget)
      {:error, :timeout} = error -> {:halt, error}
    end
  end

  defp raw_binary_value(value, until_check, _budget) when is_binary(value),
    do: {:cont, {:ok, until_check - 1}}

  defp raw_binary_value(_value, _until_check, _budget), do: {:halt, {:ok, false}}

  defp finish_raw_binary_scan({:ok, false}, _budget), do: {:ok, false}
  defp finish_raw_binary_scan({:error, :timeout} = error, _budget), do: error

  defp finish_raw_binary_scan({:ok, _until_check}, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> {:ok, true}
      {:error, :timeout} = error -> error
    end
  end

  defp encode_entries(values, codec, budget) do
    Enum.reduce_while(values, {:ok, %{}, 0}, fn entry, accumulator ->
      encode_entry(entry, accumulator, codec, budget)
    end)
  end

  defp encode_entry(entry, {:ok, encoded, 0}, codec, budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> encode_entry(entry, {:ok, encoded, @deadline_check_interval}, codec, budget)
      {:error, :timeout} = error -> {:halt, error}
    end
  end

  defp encode_entry({key, value}, {:ok, encoded, until_check}, codec, _budget) do
    encoded = Map.put(encoded, key, CodecRuntime.encode(codec, value))
    {:cont, {:ok, encoded, until_check - 1}}
  end
end
