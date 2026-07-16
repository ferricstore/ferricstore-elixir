defmodule FerricStore.Protocol.CommandLookupTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{CommandSpec, Opcodes}

  test "oversized command identifiers are rejected before normalization" do
    identifier = String.duplicate("x", 1_000_000)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, {:unknown_opcode, ^identifier}} = Opcodes.fetch(identifier)
    assert :error = CommandSpec.fetch(identifier)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
  end

  test "command lookup does not retain whitespace-normalization compatibility" do
    assert {:error, {:unknown_opcode, " GET "}} = Opcodes.fetch(" GET ")
    assert :error = CommandSpec.fetch(" FLOW.GET ")
  end
end
