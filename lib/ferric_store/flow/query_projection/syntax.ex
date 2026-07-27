defmodule FerricStore.Flow.QueryProjection.Syntax do
  @moduledoc false

  alias FerricStore.Flow.QueryText

  def validate_terminator(query) do
    trimmed = QueryText.trim(query)

    if String.ends_with?(trimmed, ";") do
      base = trimmed |> trim_one_terminator() |> QueryText.trim_trailing()

      if String.ends_with?(base, ";"), do: error(:invalid_query), else: :ok
    else
      :ok
    end
  end

  def trim_one_terminator(query) do
    size = byte_size(query)

    if size > 0 and :binary.last(query) == ?;,
      do: binary_part(query, 0, size - 1),
      else: query
  end

  def return_clause?(query) do
    query
    |> strip_quoted(false, [])
    |> IO.iodata_to_binary()
    |> then(&Regex.match?(~r/(?:^|[^A-Za-z0-9_])RETURN(?:$|[^A-Za-z0-9_])/iu, &1))
  end

  defp strip_quoted(<<>>, _quoted, acc), do: Enum.reverse(acc)

  defp strip_quoted(<<"''", rest::binary>>, true, acc),
    do: strip_quoted(rest, true, ["  " | acc])

  defp strip_quoted(<<"'", rest::binary>>, quoted, acc),
    do: strip_quoted(rest, not quoted, [" " | acc])

  defp strip_quoted(<<_byte, rest::binary>>, true, acc),
    do: strip_quoted(rest, true, [" " | acc])

  defp strip_quoted(<<byte, rest::binary>>, false, acc),
    do: strip_quoted(rest, false, [<<byte>> | acc])

  defp error(reason), do: {:error, {:invalid_flow_query_projection, reason}}
end
