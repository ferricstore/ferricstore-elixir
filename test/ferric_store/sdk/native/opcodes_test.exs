defmodule FerricStore.Protocol.OpcodesTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes

  test "resolves opcodes from atoms, protocol names, and integers" do
    assert {:ok, 0x0101} = Opcodes.fetch(:get)
    assert {:ok, 0x0101} = Opcodes.fetch("GET")
    assert {:ok, 0x010A} = Opcodes.fetch("RATELIMIT.ADD")
    assert {:ok, 0x010A} = Opcodes.fetch("ratelimit_add")
    assert {:ok, 0x0201} = Opcodes.fetch("FLOW.CREATE")
    assert {:ok, 0x0201} = Opcodes.fetch(:flow_create)
    assert {:ok, 0x000A} = Opcodes.fetch(:goaway)
    assert {:ok, 0x0010} = Opcodes.fetch("EVENT")
    assert {:ok, 0x0101} = Opcodes.fetch(0x0101)
    assert Opcodes.name(0x0101) == "GET"
    assert Opcodes.name(0x000A) == "GOAWAY"
    assert Opcodes.name(0x0010) == "EVENT"
    assert {:error, {:unknown_opcode, "NOPE"}} = Opcodes.fetch("NOPE")
  end
end
