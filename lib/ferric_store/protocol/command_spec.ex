defmodule FerricStore.Protocol.CommandSpec do
  @moduledoc false

  alias FerricStore.FailureFormatter

  alias FerricStore.Protocol.CommandName
  alias FerricStore.Protocol.CommandSpec.{Entries, FlowProperties, Metadata}

  @opcode_entries Entries.all()

  @commands Enum.map(@opcode_entries, fn {id, {name, opcode}} ->
              %{
                id: id,
                name: name,
                opcode: opcode,
                lane: if(Metadata.control_lane?(id), do: :control, else: :data),
                read_only: Metadata.read_only?(id),
                batch: Metadata.batch(id),
                flow: FlowProperties.for_command(id),
                flow_option_start: FlowProperties.option_start(id)
              }
            end)

  @by_id Map.new(@commands, &{&1.id, &1})
  @by_opcode Map.new(@commands, &{&1.opcode, &1})
  @by_name Map.new(@commands, &{&1.name, &1})
  @by_id_name Map.new(@commands, &{&1.id |> Atom.to_string() |> String.upcase(), &1})

  @spec all() :: [map()]
  def all, do: @commands

  @spec fetch(term()) :: {:ok, map()} | :error
  def fetch(identifier) do
    cond do
      is_atom(identifier) ->
        Map.fetch(@by_id, identifier)

      is_integer(identifier) ->
        Map.fetch(@by_opcode, identifier)

      is_binary(identifier) ->
        case CommandName.normalize(identifier) do
          {:ok, normalized} -> fetch_normalized_name(normalized)
          {:error, _reason} -> :error
        end

      true ->
        :error
    end
  end

  @spec fetch!(term()) :: map()
  def fetch!(identifier) do
    case fetch(identifier) do
      {:ok, command} ->
        command

      :error ->
        raise ArgumentError, "unknown command: #{FailureFormatter.inspect_term(identifier)}"
    end
  end

  @spec name(atom() | binary() | non_neg_integer()) :: binary() | nil
  def name(identifier) do
    case fetch(identifier) do
      {:ok, command} -> command.name
      :error -> nil
    end
  end

  @spec control_lane?(atom() | binary() | non_neg_integer()) :: boolean()
  def control_lane?(identifier) do
    case fetch(identifier) do
      {:ok, %{lane: :control}} -> true
      _other -> false
    end
  end

  @spec read_only?(atom() | binary() | non_neg_integer()) :: boolean()
  def read_only?(identifier) do
    case fetch(identifier) do
      {:ok, command} -> command.read_only
      :error -> false
    end
  end

  @spec batch(atom() | binary() | non_neg_integer()) :: map() | nil
  def batch(identifier) do
    case fetch(identifier) do
      {:ok, command} -> command.batch
      :error -> nil
    end
  end

  @spec flow_property?(atom() | binary() | non_neg_integer(), atom()) :: boolean()
  def flow_property?(identifier, property) do
    case fetch(identifier) do
      {:ok, command} -> MapSet.member?(command.flow, property)
      :error -> false
    end
  end

  @spec flow_option_start(atom() | binary() | non_neg_integer()) :: non_neg_integer()
  def flow_option_start(identifier) do
    case fetch(identifier) do
      {:ok, command} -> command.flow_option_start
      :error -> 1
    end
  end

  defp fetch_normalized_name(normalized) do
    with :error <- Map.fetch(@by_name, normalized) do
      Map.fetch(@by_id_name, String.replace(normalized, ".", "_"))
    end
  end
end
