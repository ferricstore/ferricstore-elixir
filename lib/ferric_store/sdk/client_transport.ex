defmodule FerricStore.SDK.ClientTransport do
  @moduledoc false

  alias FerricStore.HTTP.Client, as: HTTPClient
  alias FerricStore.SDK.Native.ClientRequests

  @spec from_url(binary(), keyword()) :: GenServer.on_start()
  def from_url(url, opts) when is_binary(url) and is_list(opts) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme}} when scheme in ["http", "https"] ->
        HTTPClient.start_link(url, opts)

      _native_or_invalid ->
        ClientRequests.from_url(url, opts)
    end
  end
end
