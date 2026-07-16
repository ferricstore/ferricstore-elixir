defmodule FerricStore.RouteKeyValidator do
  @moduledoc false

  # Keep in sync with Ferricstore.Store.Router.max_key_size/0.
  @max_bytes 65_535

  @spec max_bytes() :: pos_integer()
  def max_bytes, do: @max_bytes

  @spec validate(term()) :: {:ok, binary()} | {:error, {:invalid_route_key, term()}}
  def validate(value) when is_binary(value) and byte_size(value) <= @max_bytes,
    do: {:ok, value}

  def validate(value) when is_binary(value) do
    {:error,
     {:invalid_route_key, %{reason: :too_large, bytes: byte_size(value), limit: @max_bytes}}}
  end

  def validate(value), do: {:error, {:invalid_route_key, value}}
end
