defmodule FerricStore.SDK.Native.ConnectionTerminationTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionInfoRuntime,
    ConnectionTermination
  }

  test "fatal shutdown waits for acknowledged decoded responses before process exit" do
    tag = make_ref()
    delivery_token = make_ref()
    target = {:acknowledged_message, self(), tag}

    pending = %{
      target: target,
      opcode: Opcodes.set(),
      lane_id: 3,
      flow_controlled?: true,
      phase: :awaiting_delivery,
      delivery_token: delivery_token,
      timeout_token: make_ref(),
      timer: nil,
      chunk_bytes: 0,
      chunk_frames: 0
    }

    state = %Connection{
      event_handler: self(),
      pending: %{61 => pending},
      pending_targets: %{target => 61},
      pending_lanes: %{3 => 1},
      data_in_flight: 1
    }

    assert {:noreply, terminal_state} =
             ConnectionTermination.handle({:stop, :closed, state})

    assert terminal_state.drain.active
    assert terminal_state.drain.terminal
    assert terminal_state.pending == %{61 => pending}

    assert_receive {:ferricstore_connection_capacity, _connection,
                    %{max_in_flight: 0, max_in_flight_per_lane: 0}}

    acknowledgement =
      {:ferricstore_response_delivered, self(), tag, delivery_token}

    assert {:noreply, drained_state} =
             ConnectionInfoRuntime.handle(acknowledgement, terminal_state)

    assert drained_state.pending == %{}
    assert_receive :stop_when_drained

    assert {:stop, :normal, final_state} =
             :stop_when_drained
             |> ConnectionInfoRuntime.handle(drained_state)
             |> ConnectionTermination.handle()

    assert final_state.pending == %{}
  end

  test "ordinary fatal shutdown still stops immediately" do
    state = %Connection{}

    assert {:stop, :closed, ^state} =
             ConnectionTermination.handle({:stop, :closed, state})
  end

  test "deferred shutdown fails unresolved work before waiting for delivery acknowledgements" do
    awaiting_tag = make_ref()
    unresolved_tag = make_ref()

    awaiting = %{
      target: {:acknowledged_message, self(), awaiting_tag},
      opcode: Opcodes.set(),
      lane_id: 1,
      flow_controlled?: true,
      phase: :awaiting_delivery,
      delivery_token: make_ref(),
      timeout_token: make_ref(),
      timer: nil,
      chunk_bytes: 0,
      chunk_frames: 0
    }

    unresolved = %{
      awaiting
      | target: {:message, self(), unresolved_tag},
        lane_id: 2,
        phase: :queued,
        delivery_token: nil
    }

    state = %Connection{
      event_handler: self(),
      pending: %{71 => awaiting, 72 => unresolved},
      pending_targets: %{awaiting.target => 71, unresolved.target => 72},
      pending_lanes: %{1 => 1, 2 => 1},
      data_in_flight: 2
    }

    assert {:noreply, terminal_state} =
             ConnectionTermination.handle({:stop, :closed, state})

    assert terminal_state.pending == %{71 => awaiting}

    assert_receive {:ferricstore_connection_response, _connection, ^unresolved_tag,
                    {:error, {:transport_failed, {:connection_down, :closed}}}}

    refute_receive {:ferricstore_connection_response, _connection, ^awaiting_tag, _result}
  end

  test "deferred shutdown has a finite acknowledgement deadline" do
    tag = make_ref()
    target = {:acknowledged_message, self(), tag}

    pending = %{
      target: target,
      opcode: Opcodes.set(),
      lane_id: 4,
      flow_controlled?: true,
      phase: :awaiting_delivery,
      delivery_token: make_ref(),
      timeout_token: make_ref(),
      timer: nil,
      chunk_bytes: 0,
      chunk_frames: 0
    }

    state = %Connection{
      event_handler: self(),
      pending: %{81 => pending},
      pending_targets: %{target => 81},
      pending_lanes: %{4 => 1},
      data_in_flight: 1,
      drain: %{active: false, timeout: 10, timer: nil, token: nil}
    }

    assert {:noreply, terminal_state} =
             ConnectionTermination.handle({:stop, :closed, state})

    assert %{timer: timer, token: token} = terminal_state.drain
    assert is_reference(timer)
    assert is_reference(token)
    assert_receive {:drain_timeout, ^token}, 500

    assert {:stop, :normal, final_state} =
             {:drain_timeout, token}
             |> ConnectionInfoRuntime.handle(terminal_state)
             |> ConnectionTermination.handle()

    assert final_state.pending == %{}
    refute_receive {:ferricstore_connection_response, _connection, ^tag, {:error, _reason}}
  end
end
