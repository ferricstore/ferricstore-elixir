defmodule FerricStore.Flow.QueryResponse.Diagnostic do
  @moduledoc false

  alias FerricStore.Flow.QueryError
  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Types

  @spec from_reason(term(), term()) :: {:ok, QueryError.t()} | :error
  def from_reason(reason, raw) do
    case payload(reason) do
      value when is_map(value) ->
        case decode(value, raw) do
          {:ok, _diagnostic} = decoded -> decoded
          {:error, _reason} -> :error
        end

      _other ->
        :error
    end
  end

  @spec decode(term(), term()) :: {:ok, QueryError.t()} | {:error, term()}
  def decode(value, raw) when is_map(value) do
    with {:ok, code} <- V.required_binary(value, "code"),
         {:ok, message} <- V.required_binary(value, "message"),
         {:ok, detail} <- V.optional_binary(value, "detail"),
         {:ok, hint} <- V.optional_binary(value, "hint"),
         {:ok, retryable} <- V.required_boolean(value, "retryable"),
         {:ok, safe_to_retry} <- V.required_boolean(value, "safe_to_retry"),
         {:ok, retry_after_ms} <- V.non_negative(value, "retry_after_ms"),
         {:ok, position} <- position(Types.get(value, "position")),
         {:ok, context} <- V.optional_map(value, "context") do
      {:ok,
       %QueryError{
         code: code,
         message: message,
         detail: detail,
         hint: hint,
         retryable: retryable,
         safe_to_retry: safe_to_retry,
         retry_after_ms: retry_after_ms,
         position: position,
         context: context,
         raw: raw
       }}
    end
  end

  def decode(value, _raw), do: V.invalid(:diagnostic, value)

  defp position(nil), do: {:ok, nil}

  defp position(value) when is_map(value) do
    with {:ok, byte} <- V.positive(value, "byte"),
         {:ok, line} <- V.positive(value, "line"),
         {:ok, column} <- V.positive(value, "column") do
      {:ok, %{byte: byte, line: line, column: column}}
    end
  end

  defp position(value), do: V.invalid(:position, value)

  defp payload(%FerricStore.Error{raw: raw}), do: payload(raw)
  defp payload({_status, value}), do: payload(value)
  defp payload(value) when is_map(value), do: value
  defp payload(_reason), do: nil
end
