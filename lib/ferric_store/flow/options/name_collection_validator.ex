defmodule FerricStore.Flow.Options.NameCollectionValidator do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.Flow.Options.CollectionScan

  @options %{
    attributes_delete: [:cancel, :complete, :complete_many, :fail, :retry, :transition],
    drop_values: [:cancel, :complete, :complete_many, :fail, :signal, :transition],
    override_values: [:cancel, :complete, :complete_many, :fail, :signal, :transition]
  }
  @max_items RequestLimits.max_batch_items()

  @spec validate(atom(), keyword(), DeadlineBudget.t() | nil) :: :ok | {:error, term()}
  def validate(operation, opts, budget) do
    Enum.reduce_while(@options, :ok, fn {option, operations}, :ok ->
      if operation in operations,
        do: validate_option(Keyword.fetch(opts, option), operation, option, budget),
        else: {:cont, :ok}
    end)
  end

  defp validate_option(:error, _operation, _option, _budget), do: {:cont, :ok}
  defp validate_option({:ok, nil}, _operation, _option, _budget), do: {:cont, :ok}

  defp validate_option({:ok, names}, operation, option, budget) do
    case CollectionScan.validate(names, @max_items, &name?/1, budget) do
      {:ok, _count} -> {:cont, :ok}
      {:error, :timeout} -> {:halt, {:error, :timeout}}
      {:error, :too_large} -> {:halt, invalid(operation, option, {:maximum_items, @max_items})}
      {:error, _reason} -> {:halt, invalid(operation, option, :expected_name_list)}
    end
  end

  defp name?(value) when is_binary(value), do: value != ""
  defp name?(value) when is_atom(value), do: not is_nil(value)
  defp name?(_value), do: false

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
