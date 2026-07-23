defmodule FerricStore.Compatibility do
  @moduledoc """
  FerricStore server and native wire compatibility for this SDK release.

  FerricStore 0.10 is a breaking beta API contract. SDK 0.5 requires server
  `~> 0.10.0`; native wire framing remains protocol v1.
  """

  @minimum_server_version "0.10.0"
  @server_version_requirement "~> 0.10.0"
  @protocol_version 1

  @spec minimum_server_version() :: binary()
  def minimum_server_version, do: @minimum_server_version

  @spec server_version_requirement() :: binary()
  def server_version_requirement, do: @server_version_requirement

  @spec protocol_version() :: pos_integer()
  def protocol_version, do: @protocol_version
end
