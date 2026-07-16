defmodule FerricStore.SDK.Native.CoordinatorServerEventRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    CoordinatorServerEventRuntime,
    EventFanout
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "frames from a detached connection cannot deliver events or trigger lifecycle work" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    :ok = EventFanout.subscribe(fanout, self(), MapSet.new(["FLOW_WAKE"]))
    :ok = EventFanout.sync(fanout)

    connection = spawn(fn -> Process.sleep(:infinity) end)
    state = %State{event_fanout: fanout}
    callbacks = callbacks(self())

    assert {:noreply, ^state} =
             CoordinatorServerEventRuntime.handle(
               state,
               connection,
               0x0010,
               %{"event" => "FLOW_WAKE"},
               callbacks
             )

    assert {:noreply, ^state} =
             CoordinatorServerEventRuntime.handle(
               state,
               connection,
               0x000A,
               %{"reason" => "stale"},
               callbacks
             )

    :ok = EventFanout.sync(fanout)
    refute_receive {:ferricstore_event, _, _}
    refute_receive {:server_event_callback, _action}

    Process.exit(connection, :kill)
    assert :ok = EventFanout.stop(fanout)
  end

  test "frames from a currently tracked connection are still delivered" do
    {:ok, fanout} = EventFanout.start_link(self(), max_queue: 8)
    :ok = EventFanout.subscribe(fanout, self(), MapSet.new(["FLOW_WAKE"]))
    :ok = EventFanout.sync(fanout)

    connection = spawn(fn -> Process.sleep(:infinity) end)
    state = %State{event_fanout: fanout}
    {:ok, pool} = ConnectionPool.track(state.connection_pool, :endpoint, connection)
    state = %{state | connection_pool: pool}

    assert {:noreply, ^state} =
             CoordinatorServerEventRuntime.handle(
               state,
               connection,
               0x0010,
               %{"event" => "FLOW_WAKE"},
               callbacks(self())
             )

    :ok = EventFanout.sync(fanout)

    assert_receive {:ferricstore_event, client,
                    %{opcode: 0x0010, name: "EVENT", value: %{"event" => "FLOW_WAKE"}}}

    assert client == self()
    Process.exit(connection, :kill)
    assert :ok = EventFanout.stop(fanout)
  end

  defp callbacks(owner) do
    %{
      reconnect_event_connection: fn state ->
        send(owner, {:server_event_callback, :reconnect})
        state
      end,
      refresh_topology: fn state ->
        send(owner, {:server_event_callback, :refresh})
        state
      end,
      retire_connection: fn state, _connection ->
        send(owner, {:server_event_callback, :retire})
        state
      end
    }
  end
end
