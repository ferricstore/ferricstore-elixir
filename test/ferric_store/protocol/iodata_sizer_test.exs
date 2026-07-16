defmodule FerricStore.Protocol.IodataSizerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.IodataSizer

  test "counts nested and binary-tail iodata within a fixed budget" do
    assert IodataSizer.bounded_length(["ab", [?c | "de"]], 5) == {:ok, 5}
    assert IodataSizer.bounded_length(["ab", [?c | "de"]], 6) == {:ok, 5}
  end

  test "stops when the byte budget is exhausted" do
    assert IodataSizer.bounded_length(["ab", [?c | "de"]], 4) == {:error, :too_large}
  end

  test "rejects values that are not valid iodata" do
    assert_raise ArgumentError, ~r/invalid request iodata/, fn ->
      IodataSizer.bounded_length([256], 8)
    end

    assert_raise ArgumentError, ~r/invalid request iodata tail/, fn ->
      IodataSizer.bounded_length(["ok" | :invalid], 8)
    end
  end
end
