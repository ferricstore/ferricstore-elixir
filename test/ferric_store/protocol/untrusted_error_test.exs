defmodule FerricStore.Protocol.UntrustedErrorTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Protocol.CompactValueDecoder

  test "duplicate typed map-key errors do not retain server-provided keys" do
    key = String.duplicate("typed-secret", 100_000)
    encoded = typed_map_with_duplicate_key(key)

    assert {:error, {:duplicate_map_key, %{bytes: bytes}}} =
             result = Protocol.decode_value(encoded)

    assert bytes == byte_size(key)
    refute inspect(result) =~ "typed-secret"
  end

  test "duplicate compact map-key errors do not retain server-provided keys" do
    key = String.duplicate("compact-secret", 100_000)

    encoded =
      <<2::32, compact_binary(key)::binary, compact_binary("one")::binary,
        compact_binary(key)::binary, compact_binary("two")::binary>>

    assert {:error, {:duplicate_compact_map_key, %{bytes: bytes}}} =
             result = CompactValueDecoder.take_binary_map(encoded)

    assert bytes == byte_size(key)
    refute inspect(result) =~ "compact-secret"
  end

  defp typed_map_with_duplicate_key(key) do
    value = Protocol.encode_value(nil)

    <<6, 2::32, byte_size(key)::32, key::binary, value::binary, byte_size(key)::32, key::binary,
      value::binary>>
  end

  defp compact_binary(value), do: <<byte_size(value)::32, value::binary>>
end
