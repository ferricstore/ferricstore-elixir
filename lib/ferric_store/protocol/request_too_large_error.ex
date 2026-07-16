defmodule FerricStore.Protocol.RequestTooLargeError do
  @moduledoc false

  defexception [:size, :limit]

  @impl true
  def message(%{size: size, limit: limit}),
    do: "native request body is #{size} bytes; configured limit is #{limit} bytes"
end
