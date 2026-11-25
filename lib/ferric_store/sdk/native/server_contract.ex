defmodule FerricStore.SDK.Native.ServerContract do
  @moduledoc false

  # A server advertises the schemas and opcodes of the commands it serves. The
  # SDK checks each one it advertises against its own contract and accepts a
  # server that leaves commands out (a workflow-only server has no key-value
  # commands): a call to one fails with the server's own error.

  alias FerricStore.{Protocol.CapabilityContract, Types}

  alias FerricStore.SDK.Native.{
    FlowQueryContract,
    ServerContractCollections,
    ServerContractProtocol,
    ServerContractResponses,
    ServerContractShape,
    ServerSessionContract
  }

  @spec validate(term()) :: :ok | {:error, {:incompatible_server_contract, map()}}
  def validate(startup) when is_map(startup) do
    with :ok <- ServerContractShape.validate(startup),
         :ok <- ServerContractProtocol.validate_startup(startup),
         :ok <- ServerSessionContract.validate(startup),
         {:ok, capabilities} <- fetch_map(startup, "capabilities"),
         :ok <- ServerContractProtocol.validate_capabilities(capabilities),
         :ok <- ServerContractResponses.validate(capabilities),
         :ok <- FlowQueryContract.validate(capabilities),
         {:ok, schemas} <- fetch_map(capabilities, "schemas"),
         :ok <- validate_schemas(schemas),
         :ok <- validate_opcodes(capabilities),
         :ok <- validate_events(capabilities),
         :ok <- ServerContractProtocol.validate_auth_requirement(startup) do
      :ok
    else
      {:error, details} -> incompatible(details)
    end
  end

  def validate(startup), do: incompatible(%{invalid_startup: startup})

  @doc false
  @spec events(map()) :: [binary()]
  def events(startup) when is_map(startup) do
    case startup |> Types.get("capabilities", %{}) |> Types.get("events") do
      events when is_list(events) -> events
      _missing -> []
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
        :ok
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
      nil -> :ok
      invalid -> {:error, %{invalid_capability: "opcodes", value: invalid}}
    end
  end

  defp validate_advertised_opcodes(advertised) do
    case ServerContractCollections.index_opcodes(advertised) do
      {:ok, advertised_by_name} ->
        first_error(CapabilityContract.opcodes(), fn command ->
          validate_opcode(advertised_by_name, command)
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
        :ok
    end
  end

  defp validate_events(capabilities) do
    case Types.get(capabilities, "events") do
      nil ->
        :ok

      events when is_list(events) ->
        case ServerContractCollections.required_fields(events) do
          {:ok, _events} -> :ok
          {:error, reason} -> {:error, %{invalid_capability: "events", reason: reason}}
        end

      invalid ->
        {:error, %{invalid_capability: "events", reason: {:not_a_list, invalid}}}
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
