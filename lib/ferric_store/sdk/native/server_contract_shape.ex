defmodule FerricStore.SDK.Native.ServerContractShape do
  @moduledoc false

  alias FerricStore.Types

  @max_startup_fields 32
  @max_capability_fields 32
  @max_schemas 1_024

  @spec validate(map()) :: :ok | {:error, map()}
  def validate(startup) do
    with :ok <- bounded(startup, [], @max_startup_fields),
         do: validate_capabilities(Types.get(startup, "capabilities"))
  end

  defp validate_capabilities(capabilities) when is_map(capabilities) do
    with :ok <- bounded(capabilities, ["capabilities"], @max_capability_fields) do
      validate_schemas(Types.get(capabilities, "schemas"))
    end
  end

  defp validate_capabilities(_capabilities), do: :ok

  defp validate_schemas(schemas) when is_map(schemas),
    do: bounded(schemas, ["capabilities", "schemas"], @max_schemas)

  defp validate_schemas(_schemas), do: :ok

  defp bounded(map, _path, limit) when map_size(map) <= limit, do: :ok

  defp bounded(map, path, limit) do
    {:error,
     %{
       invalid_collection: path,
       reason: {:too_many_entries, %{limit: limit, observed: map_size(map)}}
     }}
  end
end
