defmodule FerricStore.SDK.Native.ServerCapabilitiesTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.{Codec, ConnectionEncoder, FlowControl}
  alias FerricStore.Test.NativeServer
  alias FerricStore.Transport.ServerFrameAssembler

  test "applies the advertised logical response limit and codec ownership" do
    state = %{
      configured_max_request_bytes: 1_000_000,
      max_request_bytes: 1_000_000,
      max_response_bytes: 900_000,
      configured_max_in_flight: 10,
      configured_max_in_flight_per_lane: 10,
      max_in_flight: 10,
      max_in_flight_per_lane: 10,
      max_pipeline_commands: 10,
      endpoint: %{max_response_bytes: 900_000},
      encoder: %ConnectionEncoder{control: self(), data: self()},
      server_frame_assembler:
        ServerFrameAssembler.new(
          max_streams: 4,
          max_buffer_bytes: 1_000_000,
          max_frame_bytes: 900_000,
          timeout: 1_000
        )
    }

    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{"limits" => %{"max_response_bytes" => 123_456}}
      })

    negotiated = FlowControl.apply_server_capabilities(state, startup)

    assert negotiated.max_response_bytes == 123_456
    assert negotiated.server_frame_assembler.max_frame_bytes == 123_456
    assert negotiated.encoder.compact_response_codecs[Opcodes.get()] == "kv_get_v1"

    assert negotiated.encoder.compact_response_codecs[Opcodes.flow_value_mget()] ==
             "kv_mget_v1"
  end

  test "a runtime custom response must use the codec advertised for its opcode" do
    body = <<0::16, 0x82, 0>>

    assert {:error, :unadvertised_compact_response} =
             Codec.decode_response(
               Opcodes.get(),
               0x02,
               body,
               byte_size(body),
               %{response_plan: nil, compact_codec: nil}
             )

    assert {:ok, nil} =
             Codec.decode_response(
               Opcodes.get(),
               0x02,
               body,
               byte_size(body),
               %{response_plan: nil, compact_codec: "kv_get_v1"}
             )
  end
end
