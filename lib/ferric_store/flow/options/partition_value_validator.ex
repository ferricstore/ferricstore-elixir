defmodule FerricStore.Flow.Options.PartitionValueValidator do
  @moduledoc false

  @standard_operations [
    :cancel,
    :complete,
    :complete_many,
    :create,
    :create_many,
    :fail,
    :get,
    :history,
    :retry,
    :signal,
    :transition,
    :value_put
  ]
  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    domain = domain(operation)

    case {domain, Keyword.fetch(opts, :partition_key)} do
      {nil, _value} -> :ok
      {_domain, :error} -> :ok
      {domain, {:ok, value}} -> validate_value(operation, domain, value)
    end
  end

  defp domain(operation) when operation in [:list, :search], do: :auto
  defp domain(:claim_due), do: :claim
  defp domain(operation) when operation in @standard_operations, do: :standard
  defp domain(_operation), do: nil

  defp validate_value(operation, domain, value) do
    if valid?(domain, value),
      do: :ok,
      else: {:error, {:invalid_flow_option, operation, :partition_key, expectation(domain)}}
  end

  defp valid?(:standard, value),
    do: is_nil(value) or value == :global or nonempty_binary?(value)

  defp valid?(:auto, value),
    do: is_nil(value) or value in [:auto, :any] or nonempty_binary?(value)

  defp valid?(:claim, value),
    do: is_nil(value) or value in [:auto, :any, :global] or nonempty_binary?(value)

  defp nonempty_binary?(value), do: is_binary(value) and value != ""

  defp expectation(:standard), do: :expected_partition_key
  defp expectation(:auto), do: :expected_auto_partition_key
  defp expectation(:claim), do: :expected_claim_partition_key
end
