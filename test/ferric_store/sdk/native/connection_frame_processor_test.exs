defmodule FerricStore.SDK.Native.ConnectionFrameProcessorTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionFrameProcessor

  test "an unknown nonzero response id is protocol corruption" do
    state = %{pending: %{}}
    header = %{lane_id: 1, opcode: 0x0101, request_id: 91, flags: 0}

    assert {:stop, {:unexpected_response, %{lane_id: 1, opcode: 0x0101, request_id: 91}}, ^state} =
             ConnectionFrameProcessor.process(header, <<0::16, 0>>, state)
  end
end
