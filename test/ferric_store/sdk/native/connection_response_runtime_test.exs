defmodule FerricStore.SDK.Native.ConnectionResponseRuntimeTest do
  use ExUnit.Case, async: false

  alias FerricStore.Protocol.Opcodes

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionInfoRuntime,
    ConnectionRequest,
    ConnectionResponseRuntime
  }

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
             ConnectionResponseRuntime.finish(
               state,
               request_id,
               pending,
               0,
               body,
               byte_size(body)
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result, _delivery_token}
    assert next_state.pending == %{}
    assert next_state.data_in_flight == 0
  end

  test "an expired acknowledged response returns a controlled timeout" do
    tag = make_ref()
    target = {:acknowledged_message, self(), tag}
    request_id = 42

    expired =
      pending(target,
        phase: :sent,
        deadline: System.monotonic_time(:millisecond) - 1
      )

    state = response_state(%{request_id => expired})
    body = <<0::unsigned-16, Codec.encode_value("late")::binary>>

    assert {:ok, next_state} =
             ConnectionResponseRuntime.finish(
               state,
               request_id,
               expired,
               0,
               body,
               byte_size(body)
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result, _delivery_token}
    assert next_state.pending == %{}
    assert next_state.pending_targets == %{}
    assert next_state.pending_lanes == %{}
    assert next_state.data_in_flight == 0
  end

  test "an acknowledged decode completed after its deadline returns a controlled timeout" do
    tag = make_ref()
    target = {:acknowledged_message, self(), tag}
    request_id = 43
    decode_token = make_ref()
    decode_worker = spawn(fn -> Process.sleep(:infinity) end)
    decode_monitor = Process.monitor(decode_worker)
    on_exit(fn -> Process.exit(decode_worker, :kill) end)

    expired =
      pending(target,
        phase: :decoding,
        deadline: System.monotonic_time(:millisecond) - 1,
        decode_token: decode_token,
        decode_worker: decode_worker
      )

    state = response_state(%{request_id => expired}) |> Map.put(:decode, {:response, request_id})

    assert {:ok, next_state} =
             ConnectionResponseRuntime.complete_decode(
               state,
               decode_worker,
               request_id,
               decode_token,
               {:response, %{}}
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result, ^decode_token}
    assert_receive {:DOWN, ^decode_monitor, :process, ^decode_worker, :killed}
    assert next_state.pending == %{}
    assert next_state.pending_targets == %{}
    assert next_state.pending_lanes == %{}
    assert next_state.data_in_flight == 0
    assert next_state.decode == nil
  end

  test "a timeout before an acknowledged decode completes delivers only one response" do
    tag = make_ref()
    target = {:acknowledged_message, self(), tag}
    request_id = 44
    timeout_token = make_ref()
    decode_token = make_ref()
    decode_worker = spawn(fn -> Process.sleep(:infinity) end)
    decode_monitor = Process.monitor(decode_worker)
    on_exit(fn -> Process.exit(decode_worker, :kill) end)

    decoding =
      pending(target,
        phase: :decoding,
        timeout_token: timeout_token,
        decode_token: decode_token,
        decode_worker: decode_worker
      )

    state = response_state(%{request_id => decoding})

    assert {:noreply, timed_out_state} =
             ConnectionInfoRuntime.handle(
               {:request_timeout, request_id, timeout_token},
               state
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}

    assert {:noreply, final_state} =
             ConnectionInfoRuntime.handle(
               {:ferricstore_response_decoded, decode_worker, request_id, decode_token,
                {:response, %{}}},
               timed_out_state
             )

    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result, ^decode_token}
    assert_receive {:DOWN, ^decode_monitor, :process, ^decode_worker, :killed}
    assert final_state.pending == %{}
    assert final_state.pending_targets == %{}
    assert final_state.pending_lanes == %{}
    assert final_state.data_in_flight == 0
  end

  test "a WINDOW_UPDATE timeout retains its in-flight authoritative decode" do
    tag = make_ref()
    target = {:message, self(), tag}
    request_id = 46
    timeout_token = make_ref()
    decode_token = make_ref()
    decode_worker = spawn(fn -> Process.sleep(:infinity) end)
    decode_monitor = Process.monitor(decode_worker)
    on_exit(fn -> Process.exit(decode_worker, :kill) end)

    timed_out_pending =
      pending(target,
        phase: :decoding,
        opcode: Opcodes.window_update(),
        flow_controlled?: false,
        timeout_token: timeout_token,
        decode_token: decode_token,
        decode_worker: decode_worker
      )

    state =
      response_state(%{request_id => timed_out_pending})
      |> Map.merge(%{
        data_in_flight: 0,
        event_handler: self(),
        decode: {:response, request_id},
        configured_max_in_flight: 8,
        configured_max_in_flight_per_lane: 8
      })

    assert {:noreply, discarded_state} =
             ConnectionInfoRuntime.handle(
               {:request_timeout, request_id, timeout_token},
               state
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}

    assert %{phase: :discarding_decoding, target: :discard} =
             discarded_state.pending[request_id]

    assert {:noreply, final_state} =
             ConnectionInfoRuntime.handle(
               {:ferricstore_response_decoded, decode_worker, request_id, decode_token,
                {:response, {:window_limits, %{max_in_flight: 2, max_in_flight_per_lane: 1}}}},
               discarded_state
             )

    assert final_state.pending == %{}
    assert final_state.max_in_flight == 2
    assert final_state.max_in_flight_per_lane == 1

    assert_receive {:ferricstore_connection_capacity, _connection,
                    %{max_in_flight: 2, max_in_flight_per_lane: 1}}

    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
    assert_receive {:DOWN, ^decode_monitor, :process, ^decode_worker, :killed}
  end

  test "a late authoritative decode applies limits after its deadline without success" do
    tag = make_ref()
    target = {:message, self(), tag}
    request_id = 47
    decode_token = make_ref()
    decode_worker = spawn(fn -> Process.sleep(:infinity) end)
    decode_monitor = Process.monitor(decode_worker)
    on_exit(fn -> Process.exit(decode_worker, :kill) end)

    pending =
      pending(target,
        phase: :decoding,
        opcode: Opcodes.window_update(),
        flow_controlled?: false,
        deadline: System.monotonic_time(:millisecond) - 1,
        decode_token: decode_token,
        decode_worker: decode_worker
      )

    state =
      response_state(%{request_id => pending})
      |> Map.merge(%{
        data_in_flight: 0,
        event_handler: self(),
        decode: {:response, request_id},
        configured_max_in_flight: 8,
        configured_max_in_flight_per_lane: 8
      })

    assert {:noreply, final_state} =
             ConnectionInfoRuntime.handle(
               {:ferricstore_response_decoded, decode_worker, request_id, decode_token,
                {:response, {:window_limits, %{max_in_flight: 2, max_in_flight_per_lane: 1}}}},
               state
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, {:ok, _result}}

    assert_receive {:ferricstore_connection_capacity, _connection,
                    %{max_in_flight: 2, max_in_flight_per_lane: 1}}

    refute_receive {:ferricstore_connection_capacity, _connection, _capacity}
    assert final_state.pending == %{}
    assert final_state.max_in_flight == 2
    assert final_state.max_in_flight_per_lane == 1
    assert_receive {:DOWN, ^decode_monitor, :process, ^decode_worker, :killed}
  end

  test "a final WINDOW_UPDATE frame after its deadline decodes in a discarded worker" do
    tag = make_ref()
    target = {:message, self(), tag}
    request_id = 48
    timeout_token = make_ref()
    gate = self()

    pending =
      pending(target,
        phase: :sent,
        opcode: Opcodes.window_update(),
        flow_controlled?: false,
        timeout_token: timeout_token,
        deadline: System.monotonic_time(:millisecond) - 1,
        decode_gate: gate
      )

    state =
      response_state(%{request_id => pending})
      |> Map.merge(%{
        data_in_flight: 0,
        event_handler: self(),
        configured_max_in_flight: 8,
        configured_max_in_flight_per_lane: 8
      })

    body =
      <<0::unsigned-16,
        Codec.encode_value(%{
          "accepted" => true,
          "limits" => %{
            "max_inflight_per_connection" => 2,
            "max_inflight_per_lane" => 1
          }
        })::binary>>

    assert {:ok, decoding_state} =
             ConnectionResponseRuntime.finish(
               state,
               request_id,
               pending,
               0,
               body,
               byte_size(body)
             )

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:error, :timeout}}

    assert %{phase: :discarding_decoding, decode_worker: worker, decode_token: decode_token} =
             decoding_state.pending[request_id]

    worker_monitor = Process.monitor(worker)
    assert_receive {:ferricstore_response_decoder_ready, ^worker, ^request_id, ^decode_token}

    send(
      worker,
      {:ferricstore_response_decoder_continue, gate, request_id, decode_token}
    )

    assert_receive decoded =
                     {:ferricstore_response_decoded, ^worker, ^request_id, ^decode_token, _}

    assert {:noreply, final_state} = ConnectionInfoRuntime.handle(decoded, decoding_state)
    assert final_state.pending == %{}
    assert final_state.max_in_flight == 2
    assert final_state.max_in_flight_per_lane == 1

    assert_receive {:ferricstore_connection_capacity, _connection,
                    %{max_in_flight: 2, max_in_flight_per_lane: 1}}

    refute_receive {:ferricstore_connection_response, _connection, ^tag, {:ok, _result}}
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :killed}
  end

  test "stale discarded decode metadata cannot mutate capacity or settle a caller" do
    request_id = 45
    decode_token = make_ref()
    stale_token = make_ref()
    decode_worker = spawn(fn -> Process.sleep(:infinity) end)
    decode_monitor = Process.monitor(decode_worker)
    on_exit(fn -> Process.exit(decode_worker, :kill) end)

    pending =
      pending(:discard,
        phase: :discarding_decoding,
        opcode: Opcodes.window_update(),
        flow_controlled?: false,
        decode_token: decode_token,
        decode_worker: decode_worker
      )

    state =
      response_state(%{request_id => pending})
      |> Map.merge(%{
        pending_targets: %{},
        pending_lanes: %{},
        data_in_flight: 0,
        decode: {:response, request_id},
        event_handler: self()
      })

    assert {:ok, ^state} =
             ConnectionResponseRuntime.complete_decode(
               state,
               decode_worker,
               request_id,
               stale_token,
               {:response, %{max_in_flight: 2, max_in_flight_per_lane: 1}}
             )

    refute_receive {:ferricstore_connection_capacity, _connection, _capacity}
    refute_receive {:ferricstore_connection_response, _connection, _tag, _result}
    assert Process.alive?(decode_worker)
    refute_receive {:DOWN, ^decode_monitor, :process, ^decode_worker, _reason}
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
             ConnectionResponseRuntime.finish(
               state,
               request_id,
               pending,
               0,
               body,
               byte_size(body)
             )

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

  test "acknowledged delivery keeps a draining response pending until its consumer handles it" do
    tag = make_ref()
    target = {:acknowledged_message, self(), tag}
    request_id = 43

    pending = %{
      target: target,
      opcode: Opcodes.get(),
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
      max_response_bytes: 1_024,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: true, timer: nil, token: nil}
    }

    body = <<0::unsigned-16, Codec.encode_value("committed")::binary>>

    assert {:ok, decoding_state} =
             ConnectionResponseRuntime.finish(
               state,
               request_id,
               pending,
               0,
               body,
               byte_size(body)
             )

    assert_receive decode_message

    assert {:noreply, awaiting_state} =
             ConnectionInfoRuntime.handle(decode_message, decoding_state)

    assert %{phase: :awaiting_delivery, delivery_token: delivery_token} =
             awaiting_state.pending[request_id]

    assert awaiting_state.data_in_flight == 1

    assert_receive {:ferricstore_connection_response, _connection, ^tag, {:ok, "committed"},
                    ^delivery_token}

    acknowledgement =
      {:ferricstore_response_delivered, self(), tag, delivery_token}

    assert {:noreply, final_state} =
             ConnectionInfoRuntime.handle(acknowledgement, awaiting_state)

    assert final_state.pending == %{}
    assert final_state.data_in_flight == 0
  end

  test "fatal connection failure preserves decoded responses awaiting consumer acknowledgement" do
    delivered_tag = make_ref()
    unresolved_tag = make_ref()
    delivered_target = {:acknowledged_message, self(), delivered_tag}
    unresolved_target = {:acknowledged_message, self(), unresolved_tag}

    delivered =
      pending(delivered_target,
        phase: :awaiting_delivery,
        delivery_token: make_ref(),
        timeout_token: make_ref()
      )

    unresolved =
      pending(unresolved_target,
        phase: :sent,
        timeout_token: make_ref()
      )

    state = response_state(%{51 => delivered, 52 => unresolved})
    failure = {:transport_failed, :closed}
    next_state = ConnectionRequest.fail_pending(state, failure)

    refute_receive {:ferricstore_connection_response, _connection, ^delivered_tag,
                    {:error, ^failure}}

    assert_receive {:ferricstore_connection_response, _connection, ^unresolved_tag,
                    {:error, ^failure}}

    assert next_state.pending == %{51 => delivered}
    assert next_state.pending_targets == %{delivered_target => 51}
    assert next_state.pending_lanes == %{7 => 1}
    assert next_state.data_in_flight == 1
  end

  test "a queued request timeout cannot invalidate a decoded response awaiting acknowledgement" do
    tag = make_ref()
    timeout_token = make_ref()
    target = {:acknowledged_message, self(), tag}

    delivered =
      pending(target,
        phase: :awaiting_delivery,
        delivery_token: make_ref(),
        timeout_token: timeout_token
      )

    state = response_state(%{53 => delivered})

    assert {:noreply, ^state} =
             ConnectionInfoRuntime.handle({:request_timeout, 53, timeout_token}, state)

    refute_receive {:ferricstore_connection_response, _connection, ^tag, _result}
  end

  defp pending(target, overrides) do
    Map.merge(
      %{
        target: target,
        opcode: Opcodes.get(),
        lane_id: 7,
        flow_controlled?: true,
        timer: nil,
        response_context: nil,
        deadline: System.monotonic_time(:millisecond) + 5_000,
        chunk_bytes: 0,
        chunk_frames: 0,
        chunks: []
      },
      Map.new(overrides)
    )
  end

  defp response_state(pending) do
    pending_targets =
      Map.new(pending, fn {request_id, request} -> {request.target, request_id} end)

    %{
      pending: pending,
      pending_targets: pending_targets,
      pending_lanes: %{7 => map_size(pending)},
      data_in_flight: map_size(pending),
      response_chunk_bytes: 0,
      response_chunk_frames: 0,
      decode: nil,
      max_response_bytes: 1_024,
      max_in_flight: 8,
      max_in_flight_per_lane: 8,
      drain: %{active: false}
    }
  end
end
