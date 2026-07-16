defmodule FerricStore.Flow.CodecError do
  @moduledoc false

  defexception [:codec, :operation]

  @impl true
  def message(error), do: "Flow codec #{error.operation} failed for #{inspect(error.codec)}"
end
