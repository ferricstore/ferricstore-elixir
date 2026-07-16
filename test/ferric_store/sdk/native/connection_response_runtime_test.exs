defmodule FerricStore.SDK.Native.ConnectionResponseRuntimeTest do
  use ExUnit.Case, async: false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.{Codec, ConnectionInfoRuntime, ConnectionResponseRuntime}

  test "a response decoded after its absolute deadline cannot become a late success" do
    tag = make_ref()
    target = {:message, self(), tag}
    request_id = 41

    pending = %{
      target: target,
      opcode: Opcodes.get(),
      lane_id: 7,
      flow_controlled?: true,
      timer: nil,
      response_context: nil,
      deadline: System.monotonic_time(:millisecond) - 1,
      chunk_bytes: 0,
      chunk_frames: 0
    }

    state = %{
      pending: %{request_id => pending},
      pending_targets: %{target => request_id},
      pending_lanes: %{7 => 1},
      data_in_flight: 1,
      response_chunk_bytes: 0,
      response_chunk_frames: 0,
      max_response_bytes: 1_024,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: false}
    }

    body = <<0::unsigned-16, Codec.encode_value("late")::binary>>

    assert {:ok, next_state} =
             ConnectionResponseRuntime.finish(state, request_id, pending, 0, body)

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}
    assert next_state.pending == %{}
    assert next_state.data_in_flight == 0
  end

  test "large response decoding does not run on the connection process" do
    tag = make_ref()
    target = {:message, self(), tag}
    request_id = 42

    pending = %{
      target: target,
      opcode: Opcodes.hget(),
      lane_id: 7,
      flow_controlled?: true,
      timer: nil,
      response_context: nil,
      deadline: System.monotonic_time(:millisecond) + 5_000,
      phase: :sent,
      chunk_bytes: 0,
      chunk_frames: 0,
      chunks: []
    }

    state = %{
      pending: %{request_id => pending},
      pending_targets: %{target => request_id},
      pending_lanes: %{7 => 1},
      data_in_flight: 1,
      response_chunk_bytes: 0,
      response_chunk_frames: 0,
      max_response_bytes: 2_000_000,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: false}
    }

    body = <<0::unsigned-16, Codec.encode_value(List.duplicate("value", 100_000))::binary>>
    :erlang.garbage_collect(self())
    {:reductions, before_finish} = Process.info(self(), :reductions)

    assert {:ok, decoding_state} =
             ConnectionResponseRuntime.finish(state, request_id, pending, 0, body)

    {:reductions, after_finish} = Process.info(self(), :reductions)

    assert decoding_state.pending[request_id].phase == :decoding
    assert decoding_state.decode == {:response, request_id}
    assert after_finish - before_finish < 20_000

    assert_receive decode_message, 5_000
    assert :erlang.external_size(decode_message) < 1_024
    assert {:noreply, final_state} = ConnectionInfoRuntime.handle(decode_message, decoding_state)
    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:ok, values}}
    assert length(values) == 100_000
    assert final_state.pending == %{}
    assert final_state.data_in_flight == 0
    assert final_state.decode == nil
  end
end
