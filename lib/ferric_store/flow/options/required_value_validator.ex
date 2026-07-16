defmodule FerricStore.Flow.Options.RequiredValueValidator do
  @moduledoc false

  @max_exact_integer 9_007_199_254_740_991

  @nonempty_binary_options %{
    from_state: [:transition],
    lease_token: [:complete, :fail, :retry, :transition],
    signal: [:signal],
    to_state: [:transition],
    type: [:create, :create_many, :list, :search],
    worker: [:claim_due]
  }
  @nonnegative_integer_options %{
    fencing_token: [:cancel, :complete, :fail, :retry, :transition]
  }
  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_nonempty_binaries(operation, opts),
         do: validate_nonnegative_integers(operation, opts)
  end

  defp validate_nonempty_binaries(operation, opts) do
    validate_options(
      @nonempty_binary_options,
      operation,
      opts,
      fn value ->
        is_binary(value) and value != ""
      end,
      :expected_nonempty_binary
    )
  end

  defp validate_nonnegative_integers(operation, opts) do
    validate_options(
      @nonnegative_integer_options,
      operation,
      opts,
      fn value ->
        is_integer(value) and value >= 0 and value <= @max_exact_integer
      end,
      :expected_nonnegative_exact_integer
    )
  end

  defp validate_options(option_operations, operation, opts, validator, expectation) do
    Enum.reduce_while(option_operations, :ok, fn {option, operations}, :ok ->
      if operation in operations,
        do:
          validate_present(Keyword.fetch(opts, option), operation, option, validator, expectation),
        else: {:cont, :ok}
    end)
  end

  defp validate_present(:error, _operation, _option, _validator, _expectation),
    do: {:cont, :ok}

  defp validate_present({:ok, value}, operation, option, validator, expectation) do
    if validator.(value),
      do: {:cont, :ok},
      else: {:halt, {:error, {:invalid_flow_option, operation, option, expectation}}}
  end
end
