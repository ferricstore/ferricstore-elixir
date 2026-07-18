defmodule FerricStore.Compatibility do
  @moduledoc """
  FerricStore server and native wire compatibility for this SDK release.

  FerricStore 0.8 is a breaking beta API contract. Its native wire framing
  remains protocol v1.
  """

  @minimum_server_version "0.8.0"
  @protocol_version 1

  @spec minimum_server_version() :: binary()
  def minimum_server_version, do: @minimum_server_version

  @spec protocol_version() :: pos_integer()
  def protocol_version, do: @protocol_version
end
