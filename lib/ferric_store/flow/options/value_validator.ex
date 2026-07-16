defmodule FerricStore.Flow.Options.ValueValidator do
  @moduledoc false

  alias FerricStore.Flow.Options.{
    CrossValueValidator,
    NumericValueValidator,
    RequiredValueValidator,
    StringValueValidator,
    TypeValidator
  }

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_codec(operation, opts),
         :ok <- TypeValidator.validate(operation, opts),
         :ok <- RequiredValueValidator.validate(operation, opts),
         :ok <- StringValueValidator.validate(operation, opts),
         :ok <- NumericValueValidator.validate(operation, opts),
         do: CrossValueValidator.validate(operation, opts)
  end

  defp validate_codec(operation, opts) do
    case Keyword.fetch(opts, :codec) do
      :error ->
        :ok

      {:ok, codec} ->
        if codec?(codec),
          do: :ok,
          else: invalid(operation, :codec, :expected_codec)
    end
  end

  defp codec?(codec) when is_atom(codec) do
    Code.ensure_loaded?(codec) and function_exported?(codec, :encode, 1) and
      function_exported?(codec, :decode, 1)
  end

  defp codec?(_codec), do: false

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
