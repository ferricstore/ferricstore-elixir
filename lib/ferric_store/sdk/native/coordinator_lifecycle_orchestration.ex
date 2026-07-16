defmodule FerricStore.SDK.Native.CoordinatorLifecycleOrchestration do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorConnectionOrchestrator,
    CoordinatorEventRuntime,
    CoordinatorTopologyRefreshRuntime,
    EventCall,
    EventFanout,
    EventRestoration,
    EventSubscriptions
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @default_timeout 5_000

  def initialize(state), do: CoordinatorTopologyRefreshRuntime.initialize(state)

  def abandon_refresh_waiter(state, monitor) do
    state
    |> cancel_refresh_waiter({:refresh_call, monitor})
    |> State.adjust_refresh_calls(-1)
  end

  def start_topology_refresh(state, waiter, callbacks),
    do:
      CoordinatorTopologyRefreshRuntime.start(
        state,
        waiter,
        callbacks
      )

  def finish_topology_refresh_waiter(waiter, result, state, callbacks),
    do:
      CoordinatorTopologyRefreshRuntime.finish_waiter(
        waiter,
        result,
        state,
        callbacks
      )

  def cancel_refresh_waiter(state, key),
    do: CoordinatorTopologyRefreshRuntime.cancel(state, key)

  def ensure_connection_async(state, endpoint, waiter),
    do: CoordinatorConnectionOrchestrator.ensure(state, endpoint, waiter)

  def ensure_connection_async(state, endpoint, connection_key, waiter),
    do: CoordinatorConnectionOrchestrator.ensure(state, endpoint, connection_key, waiter)

  def handle_connection_started(state, attempt, result, callbacks),
    do:
      CoordinatorConnectionOrchestrator.complete(
        state,
        attempt,
        result,
        callbacks
      )

  def remove_connection_waiter(state, key, tag, callbacks),
    do:
      CoordinatorConnectionOrchestrator.remove_waiter(
        state,
        key,
        tag,
        callbacks
      )

  def handle_connection_down(state, conn, reason, callbacks),
    do:
      CoordinatorConnectionOrchestrator.down(
        state,
        conn,
        reason,
        callbacks
      )

  def maybe_start_event_restore(state, conn, callbacks),
    do: EventRestoration.maybe_start(state, conn, callbacks.dispatch_connection)

  def reconnect_event_connection(state, callbacks),
    do:
      EventRestoration.reconnect(
        state,
        &ensure_connection_async/3,
        callbacks.dispatch_connection
      )

  def handle_subscriber_down(state, monitor, subscriber, callbacks) do
    case EventSubscriptions.subscriber(State.event_subscriptions(state), subscriber) do
      %{monitor: ^monitor} ->
        EventFanout.remove_subscriber(state.event_fanout, subscriber)

        CoordinatorEventRuntime.enqueue(
          state,
          EventCall.subscriber_down(subscriber, @default_timeout),
          callbacks
        )

      _other ->
        {:noreply, state}
    end
  end

  def refresh_topology_event(state, callbacks) do
    waiter =
      if CoordinatorTopologyRefreshRuntime.operation(state),
        do: :topology_event_followup,
        else: :topology_event

    {:noreply, state} = start_topology_refresh(state, waiter, callbacks)
    state
  end
end
