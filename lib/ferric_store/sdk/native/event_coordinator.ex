defmodule FerricStore.SDK.Native.EventCoordinator do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    EventQueue,
    EventRestore,
    EventSubscriberReservations,
    EventSubscriptions
  }

  defstruct subscriptions: %EventSubscriptions{},
            restore: %EventRestore{},
            operation: nil,
            queue: %EventQueue{},
            subscriber_reservations: %{},
            pending_only_subscribers: 0

  @type t :: %__MODULE__{
          subscriptions: EventSubscriptions.t(),
          restore: EventRestore.t(),
          operation: map() | nil,
          queue: EventQueue.t(),
          subscriber_reservations: %{optional(pid()) => pos_integer()},
          pending_only_subscribers: non_neg_integer()
        }

  @spec subscriptions(t()) :: EventSubscriptions.t()
  def subscriptions(%__MODULE__{subscriptions: subscriptions}), do: subscriptions

  @spec put_subscriptions(t(), EventSubscriptions.t()) :: t()
  def put_subscriptions(%__MODULE__{} = coordinator, %EventSubscriptions{} = subscriptions) do
    pending_only =
      pending_only_subscriber_count(coordinator.subscriber_reservations, subscriptions)

    %{coordinator | subscriptions: subscriptions, pending_only_subscribers: pending_only}
  end

  @spec subscribe(t(), pid(), MapSet.t(), pid() | nil) :: t()
  def subscribe(%__MODULE__{} = coordinator, subscriber, events, connection) do
    pending_reservation? = Map.has_key?(coordinator.subscriber_reservations, subscriber)
    previously_subscribed? = subscribed?(coordinator.subscriptions, subscriber)

    subscriptions =
      EventSubscriptions.subscribe(coordinator.subscriptions, subscriber, events, connection)

    pending_only =
      if pending_reservation? and not previously_subscribed?,
        do: max(coordinator.pending_only_subscribers - 1, 0),
        else: coordinator.pending_only_subscribers

    %{coordinator | subscriptions: subscriptions, pending_only_subscribers: pending_only}
  end

  @spec unsubscribe(t(), pid(), MapSet.t()) :: t()
  def unsubscribe(%__MODULE__{} = coordinator, subscriber, events) do
    subscriptions = EventSubscriptions.unsubscribe(coordinator.subscriptions, subscriber, events)

    became_pending_only? =
      Map.has_key?(coordinator.subscriber_reservations, subscriber) and
        subscribed?(coordinator.subscriptions, subscriber) and
        not subscribed?(subscriptions, subscriber)

    pending_only =
      if became_pending_only?,
        do: coordinator.pending_only_subscribers + 1,
        else: coordinator.pending_only_subscribers

    %{coordinator | subscriptions: subscriptions, pending_only_subscribers: pending_only}
  end

  @spec connection(t()) :: pid() | nil
  def connection(%__MODULE__{subscriptions: subscriptions}),
    do: EventSubscriptions.connection(subscriptions)

  @spec put_connection(t(), pid() | nil) :: t()
  def put_connection(%__MODULE__{} = coordinator, connection) do
    subscriptions = EventSubscriptions.put_connection(coordinator.subscriptions, connection)
    %{coordinator | subscriptions: subscriptions}
  end

  @spec clear_connection(t(), pid()) :: t()
  def clear_connection(%__MODULE__{} = coordinator, connection) do
    subscriptions = EventSubscriptions.clear_connection(coordinator.subscriptions, connection)
    %{coordinator | subscriptions: subscriptions}
  end

  @spec live_connection?(t()) :: boolean()
  def live_connection?(%__MODULE__{} = coordinator) do
    connection = connection(coordinator)
    is_pid(connection) and Process.alive?(connection)
  end

  @spec subscriptions_empty?(t()) :: boolean()
  def subscriptions_empty?(%__MODULE__{subscriptions: subscriptions}),
    do: EventSubscriptions.empty?(subscriptions)

  @spec reserve_subscriber(t(), pid(), pos_integer()) :: {:ok, t()} | :full
  def reserve_subscriber(%__MODULE__{} = coordinator, subscriber, limit)
      when is_pid(subscriber) and is_integer(limit) and limit > 0 do
    EventSubscriberReservations.reserve(coordinator, subscriber, limit)
  end

  @spec release_subscriber(t(), pid()) :: t()
  def release_subscriber(%__MODULE__{} = coordinator, subscriber) when is_pid(subscriber),
    do: EventSubscriberReservations.release(coordinator, subscriber)

  @spec subscriber_reservation_count(t()) :: non_neg_integer()
  def subscriber_reservation_count(%__MODULE__{} = coordinator),
    do: EventSubscriberReservations.count(coordinator)

  @spec restore(t()) :: EventRestore.t()
  def restore(%__MODULE__{restore: restore}), do: restore

  @spec put_restore(t(), EventRestore.t()) :: t()
  def put_restore(%__MODULE__{} = coordinator, %EventRestore{} = restore),
    do: %{coordinator | restore: restore}

  @spec operation(t()) :: map() | nil
  def operation(%__MODULE__{operation: operation}), do: operation

  @spec put_operation(t(), map() | nil) :: t()
  def put_operation(%__MODULE__{} = coordinator, operation),
    do: %{coordinator | operation: operation}

  @spec queue_size(t()) :: non_neg_integer()
  def queue_size(%__MODULE__{queue: queue}), do: EventQueue.size(queue)

  @spec queued_values(t()) :: [map()]
  def queued_values(%__MODULE__{queue: queue}), do: EventQueue.values(queue)

  @spec enqueue(t(), map()) :: t()
  def enqueue(%__MODULE__{} = coordinator, event_call),
    do: %{coordinator | queue: EventQueue.enqueue(coordinator.queue, event_call)}

  @spec pop(t(), reference()) :: {map() | nil, t()}
  def pop(%__MODULE__{} = coordinator, event_call_id) do
    {event_call, queue} = EventQueue.pop(coordinator.queue, event_call_id)
    {event_call, %{coordinator | queue: queue}}
  end

  @spec fetch(t(), reference()) :: map() | nil
  def fetch(%__MODULE__{queue: queue}, event_call_id), do: EventQueue.fetch(queue, event_call_id)

  @spec out(t()) :: {{:value, map()}, t()} | {:empty, t()}
  def out(%__MODULE__{} = coordinator) do
    case EventQueue.out(coordinator.queue) do
      {{:value, event_call}, queue} ->
        {{:value, event_call}, %{coordinator | queue: queue}}

      {:empty, queue} ->
        {:empty, %{coordinator | queue: queue}}
    end
  end

  defp pending_only_subscriber_count(reservations, subscriptions) do
    Enum.count(reservations, fn {subscriber, _count} ->
      not subscribed?(subscriptions, subscriber)
    end)
  end

  defp subscribed?(subscriptions, subscriber),
    do: not is_nil(EventSubscriptions.subscriber(subscriptions, subscriber))
end
