defmodule FerricStore.SDK.Native.CoordinatorServerEventRuntime do
  @moduledoc false

  alias FerricStore.Protocol.{CommandSpec, Opcodes}

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    EventFanout,
    EventSubscriptions
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @event_opcode CommandSpec.fetch!(:event).opcode
  @goaway_opcode CommandSpec.fetch!(:goaway).opcode

  @type callbacks :: %{
          required(:reconnect_event_connection) => (State.t() -> State.t()),
          required(:refresh_topology) => (State.t() -> State.t()),
          required(:retire_connection) => (State.t(), pid() -> State.t())
        }

  @spec handle(State.t(), pid(), non_neg_integer(), term(), callbacks()) ::
          {:noreply, State.t()}
  def handle(%State{} = state, connection, opcode, value, callbacks) do
    if ConnectionPool.connection?(state.connection_pool, connection),
      do: handle_tracked(state, connection, opcode, value, callbacks),
      else: {:noreply, state}
  end

  defp handle_tracked(state, connection, opcode, value, callbacks) do
    event = %{opcode: opcode, name: Opcodes.name(opcode), value: value}
    deliver(state.event_fanout, event, opcode)

    state = maybe_refresh_topology(state, opcode, value, callbacks)
    {:noreply, maybe_handle_goaway(state, connection, opcode, callbacks)}
  end

  defp maybe_refresh_topology(state, @event_opcode, value, callbacks) do
    if EventSubscriptions.event_kind(value) == "TOPOLOGY_CHANGED",
      do: callbacks.refresh_topology.(state),
      else: state
  end

  defp maybe_refresh_topology(state, _opcode, _value, _callbacks), do: state

  defp maybe_handle_goaway(state, connection, @goaway_opcode, callbacks) do
    state
    |> callbacks.retire_connection.(connection)
    |> State.clear_event_connection(connection)
    |> callbacks.reconnect_event_connection.()
  end

  defp maybe_handle_goaway(state, _connection, _opcode, _callbacks), do: state

  defp deliver(fanout, event, @goaway_opcode),
    do: EventFanout.dispatch(fanout, event, :broadcast)

  defp deliver(fanout, event, _opcode),
    do: EventFanout.dispatch(fanout, event, :by_kind)
end
