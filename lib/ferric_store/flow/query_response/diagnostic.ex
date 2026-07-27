defmodule FerricStore.Flow.QueryResponse.Diagnostic do
  @moduledoc false

  alias FerricStore.Flow.QueryError
  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Types

  @max_text_bytes 1_024
  @max_context_entries 16
  @max_context_list_items 32
  @max_context_key_bytes 128
  @max_context_depth 6
  @max_context_nodes 512
  @min_context_integer -0x8000_0000_0000_0000
  @max_context_integer 0x7FFF_FFFF_FFFF_FFFF

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
    with {:ok, code} <- V.bounded_binary(value, "code", @max_text_bytes),
         {:ok, message} <- V.bounded_binary(value, "message", @max_text_bytes),
         {:ok, detail} <- optional_bounded_binary(value, "detail"),
         {:ok, hint} <- optional_bounded_binary(value, "hint"),
         {:ok, retryable} <- V.required_boolean(value, "retryable"),
         {:ok, safe_to_retry} <- V.required_boolean(value, "safe_to_retry"),
         {:ok, retry_after_ms} <- V.non_negative(value, "retry_after_ms"),
         {:ok, position} <- position(Types.get(value, "position")),
         {:ok, context} <- context(Types.get(value, "context")) do
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

  defp optional_bounded_binary(value, field) do
    with {:ok, text} <- V.optional_binary(value, field),
         true <- text == nil or byte_size(text) <= @max_text_bytes do
      {:ok, text}
    else
      false -> V.invalid({:diagnostic_text, field}, Types.get(value, field))
      {:error, _reason} = error -> error
    end
  end

  defp context(nil), do: {:ok, nil}

  defp context(value) when is_map(value) and map_size(value) <= @max_context_entries do
    case validate_context_value(value, @max_context_depth, @max_context_nodes) do
      {:ok, _remaining} -> {:ok, value}
      :error -> V.invalid(:diagnostic_context, value)
    end
  end

  defp context(value), do: V.invalid(:diagnostic_context, value)

  defp validate_context_value(_value, _depth, remaining) when remaining <= 0, do: :error

  defp validate_context_value(value, _depth, remaining)
       when is_binary(value) and byte_size(value) <= @max_text_bytes do
    if String.valid?(value), do: {:ok, remaining - 1}, else: :error
  end

  defp validate_context_value(value, _depth, remaining)
       when is_integer(value) and value >= @min_context_integer and value <= @max_context_integer,
       do: {:ok, remaining - 1}

  defp validate_context_value(value, _depth, remaining)
       when is_boolean(value) or is_nil(value),
       do: {:ok, remaining - 1}

  defp validate_context_value(value, depth, remaining)
       when is_map(value) and depth > 0 and map_size(value) <= @max_context_entries do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn {key, item}, {:ok, nodes} ->
      if valid_context_key?(key) do
        case validate_context_value(item, depth - 1, nodes) do
          {:ok, _remaining} = valid -> {:cont, valid}
          :error -> {:halt, :error}
        end
      else
        {:halt, :error}
      end
    end)
  end

  defp validate_context_value(value, depth, remaining)
       when is_list(value) and depth > 0 and length(value) <= @max_context_list_items do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn item, {:ok, nodes} ->
      case validate_context_value(item, depth - 1, nodes) do
        {:ok, _remaining} = valid -> {:cont, valid}
        :error -> {:halt, :error}
      end
    end)
  end

  defp validate_context_value(_value, _depth, _remaining), do: :error

  defp valid_context_key?(key) do
    is_binary(key) and key != "" and byte_size(key) <= @max_context_key_bytes and
      String.valid?(key)
  end

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
