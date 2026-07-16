defmodule FerricStore.SDK.Native.ConnectionServerFrameRuntimeTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.{Codec, ConnectionFrameProcessor, ConnectionInfoRuntime}
  alias FerricStore.Transport.ServerFrameAssembler

  test "large server events are decoded and delivered outside the connection process" do
    values = List.duplicate("event-value", 100_000)
    body = <<0::unsigned-16, Codec.encode_value(values)::binary>>

    state = %{
      server_frame_assembler: assembler(),
      max_response_bytes: 2_000_000,
      event_handler: self(),
      drain: %{active: false, timeout: 5_000, timer: nil, token: nil},
      pending: %{},
      pending_targets: %{},
      pending_lanes: %{},
      data_in_flight: 0,
      response_chunk_bytes: 0,
      response_chunk_frames: 0
    }

    header = %{lane_id: 0, opcode: 0x0010, request_id: 0, flags: 0}
    :erlang.garbage_collect(self())
    {:reductions, before_process} = Process.info(self(), :reductions)

    assert {:ok, decoding_state} = ConnectionFrameProcessor.process(header, body, state)

    {:reductions, after_process} = Process.info(self(), :reductions)
    assert after_process - before_process < 20_000
    assert match?({:server, _decode}, decoding_state.decode)

    assert_receive decode_message, 5_000
    assert :erlang.external_size(decode_message) < 1_024

    assert {:noreply, delivering_state} =
             ConnectionInfoRuntime.handle(decode_message, decoding_state)

    assert_receive {:ferricstore_server_frame, connection, 0x0010, ^values}
    assert connection == self()

    assert_receive delivered_message

    assert {:noreply, final_state} =
             ConnectionInfoRuntime.handle(delivered_message, delivering_state)

    assert final_state.decode == nil
  end

  defp assembler do
    ServerFrameAssembler.new(
      max_streams: 2,
      max_buffer_bytes: 2_000_000,
      max_buffer_frames: 2_000,
      max_frame_bytes: 2_000_000,
      timeout: :infinity
    )
  end
end
