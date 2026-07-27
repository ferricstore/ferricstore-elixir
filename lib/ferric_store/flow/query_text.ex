defmodule FerricStore.Flow.QueryText do
  @moduledoc false

  @ascii_whitespace [32, 9, 10, 13]

  def trim(value) when is_binary(value), do: value |> trim_leading() |> trim_trailing()

  def trim_leading(<<character, rest::binary>>) when character in @ascii_whitespace,
    do: trim_leading(rest)

  def trim_leading(value) when is_binary(value), do: value

  def after_ascii_keyword(value, keyword) when is_binary(value) and is_binary(keyword) do
    size = byte_size(keyword)

    if byte_size(value) >= size do
      <<candidate::binary-size(^size), rest::binary>> = value

      if ascii_equal?(candidate, keyword) and keyword_boundary?(rest),
        do: {:ok, rest},
        else: :error
    else
      :error
    end
  end

  def trim_trailing(value) when is_binary(value), do: trim_trailing(value, byte_size(value))

  defp trim_trailing(value, size) when size > 0 do
    if :binary.at(value, size - 1) in @ascii_whitespace,
      do: trim_trailing(value, size - 1),
      else: trailing_part(value, size)
  end

  defp trim_trailing(_value, 0), do: ""

  defp trailing_part(value, size) when size == byte_size(value), do: value
  defp trailing_part(value, size), do: binary_part(value, 0, size)

  defp ascii_equal?(<<>>, <<>>), do: true

  defp ascii_equal?(<<left, left_rest::binary>>, <<right, right_rest::binary>>) do
    ascii_upper(left) == right and ascii_equal?(left_rest, right_rest)
  end

  defp ascii_equal?(_left, _right), do: false

  defp ascii_upper(character) when character in ?a..?z, do: character - 32
  defp ascii_upper(character), do: character

  defp keyword_boundary?(<<>>), do: true
  defp keyword_boundary?(<<character, _rest::binary>>), do: character in @ascii_whitespace
end
