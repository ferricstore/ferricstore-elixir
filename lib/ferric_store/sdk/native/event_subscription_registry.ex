defmodule FerricStore.SDK.Native.EventSubscriptionRegistry do
  @moduledoc false

  @spec subscribe(map(), pid(), MapSet.t(), pid() | nil) :: map()
  def subscribe(subscriptions, subscriber, events, connection) do
    changes = MapSet.difference(events, subscriber_events(subscriptions, subscriber))

    if MapSet.size(changes) == 0,
      do: subscriptions,
      else: do_subscribe(subscriptions, subscriber, changes, connection)
  end

  @spec unsubscribe(map(), pid(), MapSet.t()) :: map()
  def unsubscribe(subscriptions, subscriber, events) do
    changes = MapSet.intersection(events, subscriber_events(subscriptions, subscriber))

    if MapSet.size(changes) == 0,
      do: subscriptions,
      else: do_unsubscribe(subscriptions, subscriber, changes)
  end

  defp do_subscribe(subscriptions, subscriber, changes, connection) do
    {record, subscribers} = subscriber_record(subscriptions.subscribers, subscriber)
    record = %{record | events: MapSet.union(record.events, changes)}

    refcounts =
      Enum.reduce(changes, subscriptions.refcounts, fn event, counts ->
        Map.update(counts, event, 1, &(&1 + 1))
      end)

    %{
      subscriptions
      | subscribers: Map.put(subscribers, subscriber, record),
        refcounts: refcounts,
        connection: connection || subscriptions.connection
    }
  end

  defp do_unsubscribe(subscriptions, subscriber, changes) do
    refcounts = Enum.reduce(changes, subscriptions.refcounts, &decrement_refcount/2)
    subscribers = remove_events(subscriptions.subscribers, subscriber, changes)
    subscriptions = %{subscriptions | subscribers: subscribers, refcounts: refcounts}

    if map_size(refcounts) == 0 and MapSet.size(subscriptions.management_events) == 0,
      do: %{subscriptions | connection: nil},
      else: subscriptions
  end

  defp subscriber_record(subscribers, subscriber) do
    case Map.fetch(subscribers, subscriber) do
      {:ok, record} ->
        {record, subscribers}

      :error ->
        record = %{monitor: Process.monitor(subscriber), events: MapSet.new()}
        {record, Map.put(subscribers, subscriber, record)}
    end
  end

  defp remove_events(subscribers, subscriber, changes) do
    case Map.get(subscribers, subscriber) do
      nil ->
        subscribers

      record ->
        remaining = MapSet.difference(record.events, changes)

        if MapSet.size(remaining) == 0 do
          Process.demonitor(record.monitor, [:flush])
          Map.delete(subscribers, subscriber)
        else
          Map.put(subscribers, subscriber, %{record | events: remaining})
        end
    end
  end

  defp decrement_refcount(event, counts) do
    case Map.get(counts, event, 0) do
      count when count <= 1 -> Map.delete(counts, event)
      count -> Map.put(counts, event, count - 1)
    end
  end

  defp subscriber_events(subscriptions, subscriber) do
    case Map.get(subscriptions.subscribers, subscriber) do
      %{events: events} -> events
      nil -> MapSet.new()
    end
  end
end
