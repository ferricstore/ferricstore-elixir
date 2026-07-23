defmodule FerricStore.Flow.Options.StringValueValidator do
  @moduledoc false

  alias FerricStore.Flow.Options.{
    HistoryCursorValidator,
    ListReturnValidator,
    PartitionValueValidator
  }

  @max_ref_bytes 4_096

  @binary_or_nil %{
    correlation_id: [:create],
    event: [:history],
    idempotency_key: [:signal],
    parent_flow_id: [:create],
    root_flow_id: [:create],
    transition_to: [:signal],
    worker: [:history]
  }
  @nonempty_binary_or_nil %{
    lease_token: [:cancel],
    name: [:value_put],
    owner_flow_id: [:value_put]
  }
  @bounded_references %{
    correlation_id: [:create],
    idempotency_key: [:signal],
    name: [:value_put],
    owner_flow_id: [:value_put],
    parent_flow_id: [:create],
    root_flow_id: [:create]
  }

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_state(operation, opts),
         :ok <- PartitionValueValidator.validate(operation, opts),
         :ok <- validate_options(operation, opts, @binary_or_nil, :binary_or_nil),
         :ok <-
           validate_options(operation, opts, @nonempty_binary_or_nil, :nonempty_binary_or_nil),
         :ok <- HistoryCursorValidator.validate(operation, opts),
         :ok <- ListReturnValidator.validate(operation, opts),
         do: validate_reference_sizes(operation, opts)
  end

  defp validate_state(operation, opts) when operation in [:create, :create_many],
    do: validate_present(operation, opts, :state, :nonempty_binary)

  defp validate_state(operation, opts)
       when operation in [
              :claim_due,
              :list,
              :search,
              :terminals,
              :by_parent,
              :by_root,
              :by_correlation
            ],
       do: validate_present(operation, opts, :state, :state_selector)

  defp validate_state(_operation, _opts), do: :ok

  defp validate_options(operation, opts, specs, domain) do
    Enum.reduce_while(specs, :ok, fn {option, operations}, :ok ->
      if operation in operations do
        validation_step(validate_present(operation, opts, option, domain))
      else
        {:cont, :ok}
      end
    end)
  end

  defp validation_step(:ok), do: {:cont, :ok}
  defp validation_step({:error, _reason} = error), do: {:halt, error}

  defp validate_reference_sizes(operation, opts) do
    Enum.reduce_while(@bounded_references, :ok, fn {option, operations}, :ok ->
      value = Keyword.get(opts, option)

      if operation in operations and is_binary(value) and byte_size(value) > @max_ref_bytes,
        do: {:halt, invalid(operation, option, {:maximum_bytes, @max_ref_bytes})},
        else: {:cont, :ok}
    end)
  end

  defp validate_present(operation, opts, option, domain) do
    case Keyword.fetch(opts, option) do
      :error ->
        :ok

      {:ok, value} ->
        if valid?(domain, value), do: :ok, else: invalid(operation, option, expectation(domain))
    end
  end

  defp valid?(:nonempty_binary, value), do: is_binary(value) and value != ""
  defp valid?(:binary_or_nil, value), do: is_nil(value) or is_binary(value)

  defp valid?(:nonempty_binary_or_nil, value),
    do: is_nil(value) or valid?(:nonempty_binary, value)

  defp valid?(:state_selector, value), do: value == :any or valid?(:nonempty_binary, value)

  defp expectation(:nonempty_binary), do: :expected_nonempty_binary
  defp expectation(:binary_or_nil), do: :expected_binary_or_nil
  defp expectation(:nonempty_binary_or_nil), do: :expected_nonempty_binary_or_nil
  defp expectation(:state_selector), do: :expected_state_selector

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
