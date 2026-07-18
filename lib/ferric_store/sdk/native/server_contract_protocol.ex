defmodule FerricStore.SDK.Native.ServerContractProtocol do
  @moduledoc false

  alias FerricStore.SDK.Native.ServerContractCollections
  alias FerricStore.Types

  @protocol "ferricstore-native"
  @protocol_version 1

  @spec validate_startup(map()) :: :ok | {:error, map()}
  def validate_startup(startup) do
    case Types.get(startup, "protocol") do
      @protocol -> :ok
      protocol -> {:error, %{protocol: protocol, required_protocol: @protocol}}
    end
  end

  @spec validate_auth_requirement(map()) :: :ok | {:error, map()}
  def validate_auth_requirement(startup) do
    case Types.get(startup, "auth_required") do
      required when is_boolean(required) -> :ok
      value -> {:error, %{invalid_startup_field: "auth_required", value: value}}
    end
  end

  @spec validate_capabilities(map()) :: :ok | {:error, map()}
  def validate_capabilities(capabilities) do
    case Types.get(capabilities, "protocol_versions") do
      versions when is_list(versions) -> validate_versions(versions)
      _invalid -> invalid_versions()
    end
  end

  defp validate_versions(versions) do
    case ServerContractCollections.protocol_versions(versions, @protocol_version) do
      :ok ->
        :ok

      {:error, :missing_required_version} ->
        {:error, %{protocol_versions: versions, required_protocol_version: @protocol_version}}

      {:error, reason} ->
        {:error, %{invalid_capability: "protocol_versions", reason: reason}}
    end
  end

  defp invalid_versions do
    {:error,
     %{
       invalid_capability: "protocol_versions",
       required_protocol_version: @protocol_version
     }}
  end
end
