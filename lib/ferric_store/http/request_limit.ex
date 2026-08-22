defmodule FerricStore.HTTP.RequestLimit do
  @moduledoc false

  @max_depth 64

  @spec preflight(term(), pos_integer()) :: :ok | {:error, :request_too_large}
  def preflight(value, limit) when is_integer(limit) and limit > 0 do
    case consume(value, limit, 0) do
      {:ok, _remaining} -> :ok
      :exceeded -> {:error, :request_too_large}
    end
  end

  defp consume(value, remaining, _depth) when is_binary(value) do
    size = byte_size(value)

    if size > remaining,
      do: :exceeded,
      else: {:ok, remaining - size}
  end

  defp consume(_value, remaining, depth) when depth > @max_depth,
    do: {:ok, remaining}

  defp consume([], remaining, _depth), do: {:ok, remaining}

  defp consume([value | values], remaining, depth) do
    with {:ok, remaining} <- consume(value, remaining, depth + 1),
         do: consume(values, remaining, depth)
  end

  defp consume(value, remaining, depth) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> consume(remaining, depth + 1)
  end

  defp consume(value, remaining, depth) when is_map(value) do
    Enum.reduce_while(value, {:ok, remaining}, fn {key, item}, {:ok, remaining} ->
      case consume_pair(key, item, remaining, depth + 1) do
        {:ok, _remaining} = result -> {:cont, result}
        :exceeded -> {:halt, :exceeded}
      end
    end)
  end

  defp consume(_value, remaining, _depth), do: {:ok, remaining}

  defp consume_pair(key, value, remaining, depth) do
    with {:ok, remaining} <- consume(key, remaining, depth),
         do: consume(value, remaining, depth)
  end
end
