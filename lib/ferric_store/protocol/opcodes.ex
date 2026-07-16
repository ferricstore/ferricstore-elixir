defmodule FerricStore.Protocol.Opcodes do
  @moduledoc false

  alias FerricStore.FailureFormatter
  alias FerricStore.Protocol.{CommandName, CommandSpec}

  @commands CommandSpec.all()
  @by_atom Map.new(@commands, &{&1.id, &1.opcode})
  @by_name Map.new(@commands, &{&1.name, {&1.id, &1.opcode}})
  @by_atom_name Map.new(@commands, fn command ->
                  {command.id |> Atom.to_string() |> String.upcase(), command.opcode}
                end)
  @names_by_opcode Map.new(@commands, &{&1.opcode, &1.name})

  for %{id: id, opcode: opcode} <- @commands do
    def unquote(id)(), do: unquote(opcode)
  end

  @spec fetch(term()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fetch(identifier) when is_integer(identifier) and identifier in 0..0xFFFF,
    do: {:ok, identifier}

  def fetch(identifier) when is_atom(identifier) do
    case Map.fetch(@by_atom, identifier) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:unknown_opcode, identifier}}
    end
  end

  def fetch(identifier) when is_binary(identifier) do
    case CommandName.normalize(identifier) do
      {:ok, normalized} -> fetch_normalized(normalized, identifier)
      {:error, _reason} -> {:error, {:unknown_opcode, identifier}}
    end
  end

  def fetch(identifier), do: {:error, {:unknown_opcode, identifier}}

  @spec fetch!(term()) :: non_neg_integer()
  def fetch!(opcode) do
    case fetch(opcode) do
      {:ok, value} ->
        value

      {:error, {:unknown_opcode, identifier}} ->
        raise ArgumentError, "unknown opcode: #{FailureFormatter.inspect_term(identifier)}"
    end
  end

  @spec name(non_neg_integer()) :: binary() | nil
  def name(opcode), do: Map.get(@names_by_opcode, opcode)

  @spec read_only?(non_neg_integer() | atom() | binary()) :: boolean()
  def read_only?(opcode) do
    case fetch(opcode) do
      {:ok, value} -> CommandSpec.read_only?(value)
      {:error, _reason} -> false
    end
  end

  @spec all() :: map()
  def all, do: @by_atom

  defp fetch_normalized(normalized, identifier) do
    with :error <- Map.fetch(@by_name, normalized),
         :error <- Map.fetch(@by_atom_name, String.replace(normalized, ".", "_")) do
      {:error, {:unknown_opcode, identifier}}
    else
      {:ok, {_atom, opcode}} -> {:ok, opcode}
      {:ok, opcode} -> {:ok, opcode}
    end
  end
end
