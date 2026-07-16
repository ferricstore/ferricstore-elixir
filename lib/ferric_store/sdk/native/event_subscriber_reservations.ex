defmodule FerricStore.SDK.Native.EventSubscriberReservations do
  @moduledoc false

  alias FerricStore.SDK.Native.EventSubscriptions

  @spec reserve(map(), pid(), pos_integer()) :: {:ok, map()} | :full
  def reserve(coordinator, subscriber, limit) do
    cond do
      occupied?(coordinator, subscriber) ->
        {:ok, increment(coordinator, subscriber)}

      occupied_count(coordinator) >= limit ->
        :full

      true ->
        coordinator = %{
          coordinator
          | pending_only_subscribers: coordinator.pending_only_subscribers + 1
        }

        {:ok, increment(coordinator, subscriber)}
    end
  end

  @spec release(map(), pid()) :: map()
  def release(coordinator, subscriber) do
    case Map.get(coordinator.subscriber_reservations, subscriber) do
      nil ->
        coordinator

      1 ->
        pending_only =
          if subscribed?(coordinator.subscriptions, subscriber),
            do: coordinator.pending_only_subscribers,
            else: max(coordinator.pending_only_subscribers - 1, 0)

        %{
          coordinator
          | subscriber_reservations: Map.delete(coordinator.subscriber_reservations, subscriber),
            pending_only_subscribers: pending_only
        }

      count ->
        reservations = Map.put(coordinator.subscriber_reservations, subscriber, count - 1)
        %{coordinator | subscriber_reservations: reservations}
    end
  end

  @spec count(map()) :: non_neg_integer()
  def count(coordinator), do: map_size(coordinator.subscriber_reservations)

  defp occupied?(coordinator, subscriber) do
    Map.has_key?(coordinator.subscriber_reservations, subscriber) or
      subscribed?(coordinator.subscriptions, subscriber)
  end

  defp occupied_count(coordinator) do
    EventSubscriptions.subscriber_count(coordinator.subscriptions) +
      coordinator.pending_only_subscribers
  end

  defp increment(coordinator, subscriber) do
    reservations = Map.update(coordinator.subscriber_reservations, subscriber, 1, &(&1 + 1))
    %{coordinator | subscriber_reservations: reservations}
  end

  defp subscribed?(subscriptions, subscriber),
    do: not is_nil(EventSubscriptions.subscriber(subscriptions, subscriber))
end
