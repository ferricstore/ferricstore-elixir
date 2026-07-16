defmodule FerricStore.SDK.Native.ConnectionFrameProcessorPerformanceTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.{ConnectionFrameProcessor, ConnectionResponseDecoder}

  test "chunk reassembly does not copy the logical response on the connection process" do
    chunk = :binary.copy("x", 16_384)
    chunks = List.duplicate(chunk, 1_000)
    chunk_bytes = 16_384_000
    request_id = 91

    pending = %{
      target: {:message, self(), make_ref()},
      opcode: 0x0101,
      lane_id: 7,
      flow_controlled?: true,
      timer: nil,
      response_context: nil,
      deadline: System.monotonic_time(:millisecond) + 5_000,
      phase: :sent,
      chunks: chunks,
      chunk_bytes: chunk_bytes,
      chunk_frames: 1_000,
      flags: 0x20
    }

    state = %{
      pending: %{request_id => pending},
      pending_targets: %{},
      pending_lanes: %{7 => 1},
      data_in_flight: 1,
      response_chunk_bytes: chunk_bytes,
      response_chunk_frames: 1_000,
      max_response_bytes: 20_000_000,
      max_response_buffer_bytes: 20_000_000,
      max_response_chunk_frames: 2_000,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: false},
      event_handler: nil
    }

    header = %{lane_id: 7, opcode: 0x0101, request_id: request_id, flags: 0}
    :erlang.garbage_collect(self())
    {:reductions, before_process} = Process.info(self(), :reductions)

    assert {:ok, decoding_state} = ConnectionFrameProcessor.process(header, "z", state)

    {:reductions, after_process} = Process.info(self(), :reductions)
    assert after_process - before_process < 10_000
    assert decoding_state.decode == {:response, request_id}

    ConnectionResponseDecoder.stop_pending(decoding_state.pending)
  end
end
