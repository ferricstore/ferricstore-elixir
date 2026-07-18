defmodule FerricStore.SDK.Native.ServerContractResponses do
  @moduledoc false

  alias FerricStore.SDK.Native.ServerResponseCodecs
  alias FerricStore.Types

  @spec validate(map()) :: :ok | {:error, map()}
  def validate(capabilities) do
    with :ok <- validate_response_limit(capabilities),
         do: validate_response_codecs(capabilities)
  end

  defp validate_response_limit(capabilities) do
    case Types.get(capabilities, "limits") do
      limits when is_map(limits) ->
        case Types.get(limits, "max_response_bytes") do
          value when is_integer(value) and value > 0 -> :ok
          value -> {:error, %{invalid_capability: "limits.max_response_bytes", value: value}}
        end

      _missing ->
        {:error, %{missing_capability: "limits.max_response_bytes"}}
    end
  end

  defp validate_response_codecs(capabilities) do
    case ServerResponseCodecs.parse(capabilities) do
      {:ok, _codecs} -> :ok
      {:error, reason} -> {:error, %{invalid_compact_response_opcodes: reason}}
    end
  end
end
