defmodule FerricStore.SDK.Native.ServerContract do
  @moduledoc false

  alias FerricStore.{Protocol.CapabilityContract, Types}

  alias FerricStore.SDK.Native.{
    ServerContractCollections,
    ServerContractShape,
    ServerSessionContract
  }

  @protocol "ferricstore-native"
  @protocol_version 1
  @spec validate(term()) :: :ok | {:error, {:incompatible_server_contract, map()}}
  def validate(startup) when is_map(startup) do
    with :ok <- ServerContractShape.validate(startup),
         :ok <- validate_protocol(startup),
         :ok <- ServerSessionContract.validate(startup),
         {:ok, capabilities} <- fetch_map(startup, "capabilities"),
         :ok <- validate_protocol_version(capabilities),
         {:ok, schemas} <- fetch_map(capabilities, "schemas"),
         :ok <- validate_schemas(schemas),
         :ok <- validate_opcodes(capabilities),
         :ok <- validate_auth_requirement(startup) do
      :ok
    else
      {:error, details} -> incompatible(details)
    end
  end

  def validate(startup), do: incompatible(%{invalid_startup: startup})

  defp validate_protocol(startup) do
    case Types.get(startup, "protocol") do
      @protocol -> :ok
      protocol -> {:error, %{protocol: protocol, required_protocol: @protocol}}
    end
  end

  defp validate_auth_requirement(startup) do
    case Types.get(startup, "auth_required") do
      required when is_boolean(required) -> :ok
      value -> {:error, %{invalid_startup_field: "auth_required", value: value}}
    end
  end

  defp validate_protocol_version(capabilities) do
    versions = Types.get(capabilities, "protocol_versions")

    case versions do
      versions when is_list(versions) ->
        validate_protocol_versions(versions)

      _invalid ->
        {:error,
         %{
           invalid_capability: "protocol_versions",
           required_protocol_version: @protocol_version
         }}
    end
  end

  defp validate_protocol_versions(versions) do
    case ServerContractCollections.protocol_versions(versions, @protocol_version) do
      :ok ->
        :ok

      {:error, :missing_required_version} ->
        {:error, %{protocol_versions: versions, required_protocol_version: @protocol_version}}

      {:error, reason} ->
        {:error,
         %{
           invalid_capability: "protocol_versions",
           reason: reason
         }}
    end
  end

  defp validate_schemas(schemas) do
    CapabilityContract.required_schemas()
    |> Enum.sort_by(&elem(&1, 0))
    |> first_error(fn {command, required_fields} ->
      validate_schema(schemas, command, required_fields)
    end)
  end

  defp validate_schema(schemas, command, required_fields) do
    case Types.get(schemas, command) do
      schema when is_map(schema) ->
        with :ok <-
               validate_required_fields(
                 command,
                 required_fields,
                 Types.get(schema, "required", [])
               ) do
          validate_supported_fields(
            command,
            Map.fetch!(CapabilityContract.required_schema_fields(), command),
            Types.get(schema, "fields")
          )
        end

      _missing ->
        {:error, %{missing_schema: command}}
    end
  end

  defp validate_required_fields(command, required_fields, actual) when is_list(actual) do
    case ServerContractCollections.required_fields(actual) do
      {:ok, fields} -> compare_required_fields(command, required_fields, fields)
      {:error, reason} -> {:error, %{command: command, invalid_required_fields: reason}}
    end
  end

  defp validate_required_fields(command, required_fields, _actual),
    do: {:error, %{command: command, missing_required_fields: required_fields}}

  defp compare_required_fields(command, required_fields, actual) do
    missing = required_fields -- actual
    unsupported = actual -- required_fields

    cond do
      missing != [] ->
        {:error, %{command: command, missing_required_fields: missing}}

      unsupported != [] ->
        {:error, %{command: command, unsupported_required_fields: unsupported}}

      true ->
        :ok
    end
  end

  defp validate_supported_fields(command, required_fields, actual) when is_list(actual) do
    case ServerContractCollections.required_fields(actual) do
      {:ok, fields} -> compare_supported_fields(command, required_fields, fields)
      {:error, reason} -> {:error, %{command: command, invalid_supported_fields: reason}}
    end
  end

  defp validate_supported_fields(command, required_fields, _actual),
    do: {:error, %{command: command, missing_supported_fields: required_fields}}

  defp compare_supported_fields(command, required_fields, actual) do
    case required_fields -- actual do
      [] -> :ok
      missing -> {:error, %{command: command, missing_supported_fields: missing}}
    end
  end

  defp validate_opcodes(capabilities) do
    case Types.get(capabilities, "opcodes") do
      advertised when is_list(advertised) -> validate_advertised_opcodes(advertised)
      _missing -> {:error, %{missing_capability: "opcodes"}}
    end
  end

  defp validate_advertised_opcodes(advertised) do
    case ServerContractCollections.index_opcodes(advertised) do
      {:ok, advertised_by_name} ->
        first_error(CapabilityContract.required_opcodes(), fn required ->
          validate_opcode(advertised_by_name, required)
        end)

      {:error, reason} ->
        {:error, %{invalid_opcodes: reason}}
    end
  end

  defp validate_opcode(advertised, %{name: command, opcode: expected}) do
    case Map.fetch(advertised, command) do
      {:ok, ^expected} ->
        :ok

      {:ok, actual} ->
        {:error, %{command: command, advertised_opcode: actual, required_opcode: expected}}

      :error ->
        {:error, %{command: command, missing_opcode: expected}}
    end
  end

  defp fetch_map(map, key) do
    case Types.get(map, key) do
      value when is_map(value) -> {:ok, value}
      _missing -> {:error, %{missing_capability: key}}
    end
  end

  defp first_error(items, validator) do
    Enum.find_value(items, :ok, fn item -> error_value(validator.(item)) end)
  end

  defp error_value(:ok), do: false
  defp error_value({:error, _details} = error), do: error

  defp incompatible(details), do: {:error, {:incompatible_server_contract, details}}
end
