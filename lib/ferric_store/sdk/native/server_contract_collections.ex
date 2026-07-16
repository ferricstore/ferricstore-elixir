defmodule FerricStore.SDK.Native.ServerContractCollections do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.Protocol.CommandName
  alias FerricStore.Types

  @max_protocol_versions 16
  @max_advertised_opcodes 1_024
  @max_required_fields 128
  @max_field_name_bytes 256

  @spec protocol_versions(list(), non_neg_integer()) ::
          :ok | {:error, :improper_list | :invalid_version | term()}
  def protocol_versions(versions, required) when is_list(versions) do
    with :ok <- bounded(versions, @max_protocol_versions) do
      do_protocol_versions(versions, required, MapSet.new(), false)
    end
  end

  @spec required_fields(list()) :: {:ok, list()} | {:error, term()}
  def required_fields(fields) when is_list(fields) do
    with :ok <- bounded(fields, @max_required_fields) do
      do_required_fields(fields, MapSet.new())
    end
  end

  @spec index_opcodes(list()) :: {:ok, map()} | {:error, term()}
  def index_opcodes(opcodes) when is_list(opcodes) do
    with :ok <- bounded(opcodes, @max_advertised_opcodes) do
      do_index_opcodes(opcodes, %{}, %{})
    end
  end

  defp do_protocol_versions([], _required, _seen, true), do: :ok

  defp do_protocol_versions([], _required, _seen, false),
    do: {:error, :missing_required_version}

  defp do_protocol_versions([version | rest], required, seen, found)
       when is_integer(version) and version >= 0 and version <= 0xFFFF do
    if MapSet.member?(seen, version) do
      {:error, {:duplicate_version, version}}
    else
      do_protocol_versions(
        rest,
        required,
        MapSet.put(seen, version),
        found or version == required
      )
    end
  end

  defp do_protocol_versions([_version | _rest], _required, _seen, _found),
    do: {:error, :invalid_version}

  defp do_protocol_versions(_tail, _required, _seen, _found),
    do: {:error, :improper_list}

  defp do_required_fields([], _seen), do: {:ok, []}

  defp do_required_fields([field | rest], seen) do
    cond do
      not valid_field?(field) ->
        {:error, :invalid_field}

      MapSet.member?(seen, field) ->
        {:error, {:duplicate_field, field}}

      true ->
        case do_required_fields(rest, MapSet.put(seen, field)) do
          {:ok, fields} -> {:ok, [field | fields]}
          {:error, _reason} = error -> error
        end
    end
  end

  defp do_required_fields(_tail, _seen), do: {:error, :improper_list}

  defp do_index_opcodes([], names, _opcodes), do: {:ok, names}

  defp do_index_opcodes([entry | rest], names, opcodes) when is_map(entry) do
    name = Types.get(entry, "name")
    opcode = Types.get(entry, "opcode")

    with :ok <- validate_command_name(name),
         :ok <- validate_opcode(name, opcode, names, opcodes) do
      do_index_opcodes(
        rest,
        Map.put(names, name, opcode),
        Map.put(opcodes, opcode, name)
      )
    end
  end

  defp do_index_opcodes([_entry | _rest], _names, _opcodes),
    do: {:error, :invalid_entry}

  defp do_index_opcodes(_tail, _names, _opcodes), do: {:error, :improper_list}

  defp validate_command_name(name) do
    case CommandName.normalize(name) do
      {:ok, ^name} -> :ok
      {:ok, _normalized} -> {:error, {:invalid_name, :not_canonical}}
      {:error, reason} -> {:error, {:invalid_name, reason}}
    end
  end

  defp validate_opcode(name, opcode, names, opcodes) do
    cond do
      not is_integer(opcode) or opcode < 0 or opcode > 0xFFFF ->
        {:error, {:invalid_opcode, name, opcode}}

      Map.has_key?(names, name) ->
        {:error, {:duplicate_name, name}}

      Map.has_key?(opcodes, opcode) ->
        {:error, {:duplicate_opcode, name, opcode}}

      true ->
        :ok
    end
  end

  defp valid_field?(field),
    do:
      is_binary(field) and field != "" and byte_size(field) <= @max_field_name_bytes and
        String.valid?(field)

  defp too_many(limit),
    do: {:error, {:too_many_entries, %{limit: limit, observed: limit + 1}}}

  defp bounded(items, limit) do
    case BoundedList.count(items, limit) do
      {:ok, _count} -> :ok
      {:error, {:limit_exceeded, _observed}} -> too_many(limit)
      {:error, :improper_list} -> {:error, :improper_list}
    end
  end
end
