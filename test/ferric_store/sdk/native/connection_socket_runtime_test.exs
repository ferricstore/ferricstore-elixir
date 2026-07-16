defmodule FerricStore.SDK.Native.ConnectionSocketRuntimeTest do
  use ExUnit.Case, async: false

  alias FerricStore.Protocol

  alias FerricStore.SDK.Native.{
    ConnectionResponseDecoder,
    ConnectionSocketRuntime
  }

  alias FerricStore.Transport.FrameStream

  test "frame processing pauses while a response is decoded off-process" do
    first_body = response_body(List.duplicate("first", 100_000))
    second_body = response_body("second")
    first_frame = response_frame(1, first_body)
    second_frame = response_frame(2, second_body)

    state = %{
      buffer: FrameStream.append(FrameStream.new(), first_frame <> second_frame),
      pending: %{1 => pending(1), 2 => pending(2)},
      pending_targets: %{},
      pending_lanes: %{7 => 2},
      data_in_flight: 2,
      response_chunk_bytes: 0,
      response_chunk_frames: 0,
      max_frame_bytes: 2_000_000,
      max_response_bytes: 2_000_000,
      max_response_buffer_bytes: 2_000_000,
      max_response_chunk_frames: 1_024,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: false},
      event_handler: nil
    }

    assert {:noreply, decoding_state} = ConnectionSocketRuntime.continue(state)
    assert decoding_state.decode == {:response, 1}
    assert decoding_state.pending[1].phase == :decoding
    assert decoding_state.pending[2].phase == :sent
    assert FrameStream.byte_size(decoding_state.buffer) == byte_size(second_frame)

    ConnectionResponseDecoder.stop_pending(decoding_state.pending)
  end

  defp pending(request_id) do
    %{
      target: {:message, self(), make_ref()},
      opcode: 0x0101,
      lane_id: 7,
      flow_controlled?: true,
      timer: nil,
      response_context: nil,
      deadline: System.monotonic_time(:millisecond) + 5_000,
      phase: :sent,
      chunks: [],
      chunk_bytes: 0,
      chunk_frames: 0,
      flags: 0,
      request_id: request_id
    }
  end

  defp response_body(value), do: <<0::unsigned-16, Protocol.encode_value(value)::binary>>

  defp response_frame(request_id, body) do
    <<"FSNP", 0x81, 0, 7::unsigned-32, 0x0101::unsigned-16, request_id::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
  end
end
