defmodule FerricStore.SDK.Native.ConnectionFrameProcessorTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionFrameProcessor

  test "an unknown nonzero response id is protocol corruption" do
    state = %{pending: %{}}
    header = %{lane_id: 1, opcode: 0x0101, request_id: 91, flags: 0}

    assert {:stop, {:unexpected_response, %{lane_id: 1, opcode: 0x0101, request_id: 91}}, ^state} =
             ConnectionFrameProcessor.process(header, <<0::16, 0>>, state)
  end

  test "interleaved chunks remain isolated by lane, opcode, and request id" do
    first = pending(1, 0x0104)
    second = pending(2, 0x020C)
    state = state(%{41 => first, 42 => second})

    assert {:ok, state} =
             ConnectionFrameProcessor.process(
               %{lane_id: 1, opcode: 0x0104, request_id: 41, flags: 0x20},
               "mget-",
               state
             )

    assert {:ok, state} =
             ConnectionFrameProcessor.process(
               %{lane_id: 2, opcode: 0x020C, request_id: 42, flags: 0x20},
               "flow-",
               state
             )

    assert state.pending[41].chunks == ["mget-"]
    assert state.pending[42].chunks == ["flow-"]

    assert {:stop,
            {:protocol_response_mismatch,
             %{
               expected: {1, 0x0104, 41},
               actual: {2, 0x020C, 41}
             }}, ^state} =
             ConnectionFrameProcessor.process(
               %{lane_id: 2, opcode: 0x020C, request_id: 41, flags: 0},
               "wrong-stream",
               state
             )
  end

  defp pending(lane_id, opcode) do
    %{
      target: {:message, self(), make_ref()},
      opcode: opcode,
      lane_id: lane_id,
      flow_controlled?: true,
      timer: nil,
      response_context: nil,
      deadline: :infinity,
      phase: :sent,
      chunks: [],
      chunk_bytes: 0,
      chunk_frames: 0,
      flags: 0
    }
  end

  defp state(pending) do
    %{
      pending: pending,
      response_chunk_bytes: 0,
      response_chunk_frames: 0,
      max_response_bytes: 1_024,
      max_response_buffer_bytes: 2_048,
      max_response_chunk_frames: 16
    }
  end
end
