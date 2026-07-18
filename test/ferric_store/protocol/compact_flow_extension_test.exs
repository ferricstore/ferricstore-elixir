defmodule FerricStore.Protocol.CompactFlowExtensionTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol

  test "unknown numeric Flow record extensions are skipped without losing named extensions" do
    payload =
      IO.iodata_to_binary([
        <<0x84, 3::32, 1>>,
        Protocol.encode_value("flow-1"),
        <<42>>,
        Protocol.encode_value(%{"future" => true}),
        <<0, byte_size("max_active_ms")::32, "max_active_ms">>,
        Protocol.encode_value(60_000)
      ])

    assert {:ok, %{"id" => "flow-1", "max_active_ms" => 60_000}, ""} =
             Protocol.CompactValueDecoder.take_flow_record(payload)
  end
end
