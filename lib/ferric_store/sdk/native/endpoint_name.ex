defmodule FerricStore.SDK.Native.EndpointName do
  @moduledoc false

  alias FerricStore.BoundedList

  @max_bytes 255
  @invalid_characters ~r/[\s\x00-\x1F\x7F]/u

  @spec normalize(term()) :: {:ok, binary()} | {:error, :invalid_endpoint_name}
  def normalize(value) when is_binary(value), do: normalize_binary(value)

  def normalize(value) when is_list(value) do
    with {:ok, _count} <- BoundedList.count(value, @max_bytes),
         {:ok, binary} <- to_binary(value) do
      normalize_binary(binary)
    else
      _invalid -> {:error, :invalid_endpoint_name}
    end
  end

  def normalize(_value), do: {:error, :invalid_endpoint_name}

  @spec normalize!(binary() | charlist()) :: binary()
  def normalize!(value) do
    case normalize(value) do
      {:ok, normalized} -> normalized
      {:error, :invalid_endpoint_name} -> raise ArgumentError, "invalid endpoint name"
    end
  end

  @spec valid?(term()) :: boolean()
  def valid?(value), do: match?({:ok, _normalized}, normalize(value))

  defp normalize_binary(value) do
    if byte_size(value) <= @max_bytes and String.valid?(value) do
      normalized = value |> String.trim() |> String.downcase()

      if normalized != "" and byte_size(normalized) <= @max_bytes and
           not Regex.match?(@invalid_characters, normalized) do
        {:ok, normalized}
      else
        {:error, :invalid_endpoint_name}
      end
    else
      {:error, :invalid_endpoint_name}
    end
  end

  defp to_binary(value) do
    {:ok, List.to_string(value)}
  rescue
    _error -> {:error, :invalid_endpoint_name}
  end
end
