defmodule FerricStore.Flow.QueryResponse.PageValidation do
  @moduledoc false

  alias FerricStore.Types

  @maximum_cursor_bytes 4_096

  def validate(value) when is_map(value) do
    has_more = Types.get(value, "has_more")
    cursor = Types.get(value, "cursor")

    with :ok <- validate_has_more(has_more),
         :ok <- validate_cursor(cursor),
         :ok <- validate_consistency(has_more, cursor, value) do
      {:ok, %{has_more: has_more, cursor: cursor}}
    end
  end

  def validate(value), do: invalid(:page, value)

  defp validate_has_more(value) when is_boolean(value), do: :ok
  defp validate_has_more(value), do: invalid(:page_has_more, value)

  defp validate_cursor(nil), do: :ok

  defp validate_cursor(value) when is_binary(value) do
    if byte_size(value) <= @maximum_cursor_bytes and String.valid?(value) and
         String.starts_with?(value, "fqc1_") do
      :ok
    else
      invalid(:page_cursor, value)
    end
  end

  defp validate_cursor(value), do: invalid(:page_cursor, value)

  defp validate_consistency(has_more, cursor, _value)
       when has_more == is_binary(cursor),
       do: :ok

  defp validate_consistency(_has_more, _cursor, value),
    do: invalid(:page_consistency, value)

  defp invalid(field, value), do: {:error, {:invalid_flow_query_response, field, value}}
end
