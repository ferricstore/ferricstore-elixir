defmodule FerricStore.ProtocolPropertyTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Protocol.ValueCodec
  alias FerricStore.SDK.Native.Codec

  test "typed values round-trip across deterministic randomized shapes" do
    :rand.seed(:exsss, {10_031, 20_063, 30_089})

    Enum.each(1..500, fn _iteration ->
      value = random_value(3)
      encoded = Protocol.encode_value(value)
      encoded_size = byte_size(encoded)

      assert {:ok, ^value, <<>>} = Protocol.decode_value(encoded)
      assert {:ok, ^encoded_size} = ValueCodec.encoded_size(value, encoded_size)
      assert {:error, :too_large} = ValueCodec.encoded_size(value, encoded_size - 1)
    end)
  end

  test "malformed response bodies never raise either decoder" do
    :rand.seed(:exsss, {41_009, 51_031, 61_049})

    Enum.each(1..1_000, fn _iteration ->
      bytes = random_binary(:rand.uniform(128) - 1)
      opcode = :rand.uniform(0xFFFF) - 1
      flags = Enum.random([0, 0x02])

      assert response_result?(Protocol.decode_response_body(flags, opcode, bytes))
      assert response_result?(Codec.decode_response(opcode, flags, bytes, 1_024))
    end)
  end

  defp response_result?({:ok, _value}), do: true
  defp response_result?({:error, _reason}), do: true
  defp response_result?({_status, _value}), do: true
  defp response_result?(_result), do: false

  defp random_value(0), do: random_scalar()

  defp random_value(depth) do
    case :rand.uniform(7) do
      1 -> random_scalar()
      2 -> random_scalar()
      3 -> Enum.map(indices(:rand.uniform(4) - 1), fn _ -> random_value(depth - 1) end)
      4 -> random_map(depth - 1)
      5 -> random_binary(:rand.uniform(32) - 1)
      6 -> :rand.uniform(10_000) - 5_000
      7 -> (:rand.uniform() - 0.5) * 10_000
    end
  end

  defp random_scalar do
    case :rand.uniform(6) do
      1 -> nil
      2 -> true
      3 -> false
      4 -> :rand.uniform(10_000) - 5_000
      5 -> (:rand.uniform() - 0.5) * 10_000
      6 -> random_binary(:rand.uniform(32) - 1)
    end
  end

  defp random_map(depth) do
    count = :rand.uniform(4) - 1

    Enum.reduce(indices(count), %{}, fn index, acc ->
      Map.put(acc, "key-#{index}", random_value(depth))
    end)
  end

  defp random_binary(0), do: <<>>

  defp random_binary(size) do
    for _ <- 1..size, into: <<>>, do: <<:rand.uniform(256) - 1>>
  end

  defp indices(0), do: []
  defp indices(count), do: 1..count
end
