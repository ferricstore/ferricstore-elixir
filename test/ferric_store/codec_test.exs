defmodule FerricStore.CodecTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term

  test "term decoding requires one exact uncompressed external term" do
    encoded = Term.encode(%{job: 42})
    assert Term.decode(encoded) == %{job: 42}

    assert_raise ArgumentError, ~r/trailing bytes/, fn ->
      Term.decode(encoded <> "untrusted-trailer")
    end

    compressed = :erlang.term_to_binary(List.duplicate("expanded", 1_000), compressed: 9)

    assert_raise ArgumentError, ~r/compressed external terms/, fn ->
      Term.decode(compressed)
    end
  end
end
