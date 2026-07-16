defmodule FerricStore.Protocol.IodataSizer do
  @moduledoc false

  @spec bounded_length(iodata(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, :too_large}
  def bounded_length(iodata, limit) when is_integer(limit) and limit >= 0 do
    case count(iodata, limit) do
      {:ok, remaining} -> {:ok, limit - remaining}
      {:error, :too_large} = error -> error
    end
  end

  defp count(value, remaining) when is_binary(value),
    do: reserve(remaining, byte_size(value))

  defp count(value, remaining) when is_integer(value) and value in 0..255,
    do: reserve(remaining, 1)

  defp count(value, remaining) when is_list(value), do: count_list(value, remaining)

  defp count(value, _remaining),
    do: raise(ArgumentError, "invalid request iodata: #{inspect(value)}")

  defp count_list([], remaining), do: {:ok, remaining}

  defp count_list([head | tail], remaining) do
    with {:ok, remaining} <- count(head, remaining), do: count_tail(tail, remaining)
  end

  defp count_tail(tail, remaining) when is_list(tail), do: count_list(tail, remaining)
  defp count_tail(tail, remaining) when is_binary(tail), do: count(tail, remaining)

  defp count_tail(tail, _remaining),
    do: raise(ArgumentError, "invalid request iodata tail: #{inspect(tail)}")

  defp reserve(remaining, size) when size <= remaining, do: {:ok, remaining - size}
  defp reserve(_remaining, _size), do: {:error, :too_large}
end
