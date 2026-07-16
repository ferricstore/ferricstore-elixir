defmodule FerricStore.Protocol.ResponsePlan do
  @moduledoc false

  alias FerricStore.Protocol.{CommandName, CommandSpec}
  alias FerricStore.Types

  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode
  @claim_opcode CommandSpec.fetch!(:flow_claim_due).opcode
  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode

  @claim_modes %{
    "JOBS_COMPACT" => :base,
    "JOB_COMPACT" => :base,
    "JOBS_COMPACT_ATTRS" => :attrs,
    "JOB_COMPACT_ATTRS" => :attrs,
    "JOBS_COMPACT_ATTRIBUTES" => :attrs,
    "JOB_COMPACT_ATTRIBUTES" => :attrs,
    "JOBS_COMPACT_STATE" => :state,
    "JOB_COMPACT_STATE" => :state,
    "JOBS_COMPACT_WITH_STATE" => :state,
    "JOB_COMPACT_WITH_STATE" => :state,
    "JOBS_COMPACT_STATE_ATTRS" => :state_attrs,
    "JOB_COMPACT_STATE_ATTRS" => :state_attrs,
    "JOBS_COMPACT_WITH_STATE_ATTRS" => :state_attrs,
    "JOB_COMPACT_WITH_STATE_ATTRS" => :state_attrs,
    "JOBS_COMPACT_STATE_ATTRIBUTES" => :state_attrs,
    "JOB_COMPACT_STATE_ATTRIBUTES" => :state_attrs,
    "JOBS_COMPACT_WITH_STATE_ATTRIBUTES" => :state_attrs,
    "JOB_COMPACT_WITH_STATE_ATTRIBUTES" => :state_attrs
  }

  @type claim_mode :: :base | :attrs | :state | :state_attrs | :unknown
  @type t :: [claim_mode()] | claim_mode() | nil

  @spec build(non_neg_integer(), term()) :: t()
  def build(@pipeline_opcode, payload) do
    case Types.get(payload, "commands") do
      commands when is_list(commands) -> build_commands(commands, [])
      _missing_or_invalid -> nil
    end
  end

  def build(@claim_opcode, payload), do: claim_mode(Types.get(payload, "return"))

  def build(@command_exec_opcode, payload), do: raw_command_mode(payload)

  def build(_opcode, _payload), do: nil

  defp build_commands([], acc), do: Enum.reverse(acc)

  defp build_commands([command | commands], acc),
    do: build_commands(commands, [command_mode(command) | acc])

  defp build_commands(_improper_tail, _acc), do: nil

  defp command_mode(command) do
    opcode = Types.get(command, "opcode")
    body = Types.get(command, "body", %{})

    cond do
      opcode == @claim_opcode -> claim_mode(Types.get(body, "return"))
      opcode == @command_exec_opcode -> raw_command_mode(body)
      true -> :unknown
    end
  end

  defp raw_command_mode(body) do
    with {:ok, "FLOW.CLAIM_DUE"} <- CommandName.normalize(Types.get(body, "command")),
         args when is_list(args) <- Types.get(body, "args", []) do
      raw_return_mode(args)
    else
      _not_compact_claim -> :unknown
    end
  end

  defp raw_return_mode([]), do: :unknown

  defp raw_return_mode([option, value | rest]) do
    case CommandName.normalize(option) do
      {:ok, "RETURN"} -> claim_mode(value)
      _other -> raw_return_mode([value | rest])
    end
  end

  defp raw_return_mode(_improper_or_single), do: :unknown

  defp claim_mode(value) do
    case CommandName.normalize(value) do
      {:ok, normalized} -> Map.get(@claim_modes, normalized, :unknown)
      {:error, _reason} -> :unknown
    end
  end
end
