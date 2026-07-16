defmodule FerricStore.Protocol.CommandName do
  @moduledoc false

  @max_bytes 1_024

  @spec normalize(term()) :: {:ok, binary()} | {:error, atom()}
  def normalize(name) when is_binary(name) do
    cond do
      name == "" ->
        {:error, :empty}

      byte_size(name) > @max_bytes ->
        {:error, :too_long}

      not String.valid?(name) ->
        {:error, :invalid_utf8}

      true ->
        normalized(name)
    end
  end

  def normalize(_name), do: {:error, :expected_binary}

  defp normalized(name) do
    normalized = String.upcase(name)
    if byte_size(normalized) <= @max_bytes, do: {:ok, normalized}, else: {:error, :too_long}
  end
end
