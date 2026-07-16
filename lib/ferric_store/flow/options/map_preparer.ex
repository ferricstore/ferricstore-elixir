defmodule FerricStore.Flow.Options.MapPreparer do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Types

  @map_options %{
    attributes: [
      :cancel,
      :complete,
      :create,
      :create_many,
      :fail,
      :list,
      :retry,
      :search,
      :transition
    ],
    attributes_merge: [:cancel, :complete, :complete_many, :fail, :retry, :transition],
    state_meta: [
      :cancel,
      :complete,
      :complete_many,
      :create,
      :create_many,
      :fail,
      :retry,
      :search,
      :transition
    ],
    value_refs: [
      :cancel,
      :complete,
      :complete_many,
      :create,
      :create_many,
      :fail,
      :signal,
      :transition
    ],
    values: [
      :cancel,
      :complete,
      :complete_many,
      :create,
      :create_many,
      :fail,
      :signal,
      :transition
    ]
  }

  @spec prepare(atom(), keyword()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts) do
    prepare_options(operation, opts, nil)
  end

  @spec prepare(atom(), keyword(), DeadlineBudget.t()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts, %DeadlineBudget{} = budget) do
    prepare_options(operation, opts, budget)
  end

  defp prepare_options(operation, opts, budget) do
    Enum.reduce_while(@map_options, {:ok, opts}, fn {option, operations}, {:ok, prepared} ->
      if operation in operations do
        prepare_option(prepared, operation, option, budget)
      else
        {:cont, {:ok, prepared}}
      end
    end)
  end

  defp prepare_option(opts, operation, option, budget) do
    case Keyword.fetch(opts, option) do
      :error ->
        {:cont, {:ok, opts}}

      {:ok, nil} ->
        {:cont, {:ok, opts}}

      {:ok, value} when is_map(value) ->
        normalize_option(opts, operation, option, value, budget)

      {:ok, _value} ->
        {:halt, invalid(operation, option, :expected_map)}
    end
  end

  defp normalize_option(opts, operation, :values = option, value, budget) do
    finish_normalization(
      normalize_map_keys(value, budget),
      opts,
      operation,
      option
    )
  end

  defp normalize_option(opts, operation, option, value, budget) do
    finish_normalization(normalize_map(value, budget), opts, operation, option)
  end

  defp finish_normalization({:ok, normalized}, opts, _operation, option) do
    prepared = PreparedMap.new(normalized)
    {:cont, {:ok, Keyword.replace!(opts, option, prepared)}}
  end

  defp finish_normalization({:error, :timeout} = error, _opts, _operation, _option),
    do: {:halt, error}

  defp finish_normalization({:error, reason}, _opts, operation, option),
    do: {:halt, invalid(operation, option, reason)}

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}

  defp normalize_map_keys(value, nil), do: Types.normalize_map_keys_result(value)

  defp normalize_map_keys(value, %DeadlineBudget{} = budget),
    do: Types.normalize_map_keys_result(value, budget)

  defp normalize_map(value, nil), do: Types.normalize_map_result(value)

  defp normalize_map(value, %DeadlineBudget{} = budget),
    do: Types.normalize_map_result(value, budget)
end
