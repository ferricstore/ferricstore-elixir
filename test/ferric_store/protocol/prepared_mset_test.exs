defmodule FerricStore.Protocol.PreparedMSetTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.PreparedMSet

  test "over-limit pair collections return the typed size error" do
    pairs = List.duplicate({"key", "value"}, 100_001)

    assert {:error, :too_large} = PreparedMSet.prepare(pairs, 10_000_000, [])
  end
end
