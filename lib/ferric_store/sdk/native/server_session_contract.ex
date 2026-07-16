defmodule FerricStore.SDK.Native.ServerSessionContract do
  @moduledoc false

  alias FerricStore.Types

  @protocol_version 1
  @compression "none"

  @spec validate(map()) :: :ok | {:error, map()}
  def validate(startup) do
    with :ok <- validate_version(Types.get(startup, "version")) do
      validate_compression(Types.get(startup, "compression"))
    end
  end

  defp validate_version(@protocol_version), do: :ok

  defp validate_version(version),
    do: {:error, %{protocol_version: version, required_protocol_version: @protocol_version}}

  defp validate_compression(@compression), do: :ok

  defp validate_compression(compression),
    do: {:error, %{compression: compression, required_compression: @compression}}
end
