defmodule FerricStore.SDK.Native.EventCommit do
  @moduledoc false

  alias FerricStore.SDK.Native.Coordinator.State
  alias FerricStore.SDK.Native.{EventFanout, EventSubscriptions}

  @spec subscribe(State.t(), pid(), MapSet.t(), pid() | nil) :: State.t()
  def subscribe(state, subscriber, events, connection) do
    previous = EventSubscriptions.subscriber(State.event_subscriptions(state), subscriber)
    state = State.subscribe_events(state, subscriber, events, connection)

    state =
      case {previous, EventSubscriptions.subscriber(State.event_subscriptions(state), subscriber)} do
        {nil, %{monitor: monitor}} ->
          State.put_lifecycle_monitor(state, monitor, {:event_subscriber, subscriber})

        _existing_or_missing ->
          state
      end

    EventFanout.subscribe(state.event_fanout, subscriber, events)
    state
  end

  @spec unsubscribe(State.t(), pid(), MapSet.t()) :: State.t()
  def unsubscribe(state, subscriber, events) do
    previous = EventSubscriptions.subscriber(State.event_subscriptions(state), subscriber)
    state = State.unsubscribe_events(state, subscriber, events)

    state =
      case {previous, EventSubscriptions.subscriber(State.event_subscriptions(state), subscriber)} do
        {%{monitor: monitor}, nil} ->
          State.delete_lifecycle_monitor(state, monitor, {:event_subscriber, subscriber})

        _remaining_or_missing ->
          state
      end

    EventFanout.unsubscribe(state.event_fanout, subscriber, events)
    state
  end
end
