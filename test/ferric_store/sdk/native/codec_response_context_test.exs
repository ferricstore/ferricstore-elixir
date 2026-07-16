defmodule FerricStore.SDK.Native.CodecResponseContextTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.Codec

  test "pipeline response plans reach the compact decoder through the native codec" do
    claim = compact_base_claim("flow", "lease", 7)
    compact_pipeline = <<0x95, 3::32, claim::binary, 0, 0, 0, 0>>
    body = <<0::16, compact_pipeline::binary>>

    assert {:ok, [["ok", ["flow", nil, "lease", 7]], ["ok", nil], ["ok", nil]]} =
             Codec.decode_response(
               Opcodes.pipeline(),
               0x02,
               body,
               byte_size(body),
               [:base, :unknown, :unknown]
             )

    assert {:error, :compact_pipeline_plan_mismatch} =
             Codec.decode_response(Opcodes.pipeline(), 0x02, body, byte_size(body), [:unknown])
  end

  test "direct claim response plans select one exact row layout" do
    pipeline_item = compact_base_claim("flow", "lease", 7)
    claim = binary_part(pipeline_item, 2, byte_size(pipeline_item) - 2)
    body = <<0::16, 0x80, 1::32, claim::binary>>

    assert {:ok, [["flow", nil, "lease", 7]]} =
             Codec.decode_response(
               Opcodes.flow_claim_due(),
               0x02,
               body,
               byte_size(body),
               :base
             )

    assert {:error, :invalid_compact_claim_job} =
             Codec.decode_response(
               Opcodes.flow_claim_due(),
               0x02,
               body,
               byte_size(body),
               :state
             )
  end

  test "raw command claim response plans select the exact compact row layout" do
    pipeline_item = compact_base_claim("flow", "lease", 7)
    claim = binary_part(pipeline_item, 2, byte_size(pipeline_item) - 2)
    body = <<0::16, 0x80, 1::32, claim::binary>>

    assert {:ok, [["flow", nil, "lease", 7]]} =
             Codec.decode_response(
               Opcodes.command_exec(),
               0x02,
               body,
               byte_size(body),
               :base
             )
  end

  defp compact_base_claim(id, lease, fencing_token) do
    IO.iodata_to_binary([
      <<0, 4, byte_size(id)::32>>,
      id,
      <<0xFFFF_FFFF::32, byte_size(lease)::32>>,
      lease,
      <<fencing_token::signed-64>>
    ])
  end
end
