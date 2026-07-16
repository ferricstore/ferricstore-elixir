defmodule FerricStore.SDK.Native.EventSubscriptionWirePolicy do
  @moduledoc false

  alias FerricStore.SDK.Native.EventIdentifier

  @all_events {:ferricstore, :all_events}
  @supported_events EventIdentifier.supported()
  @supported_event_set MapSet.new(@supported_events)

  def normalize([]), do: MapSet.new([@all_events])

  def normalize(events) do
    Enum.reduce(events, MapSet.new(), fn event, normalized ->
      case EventIdentifier.normalize(event) do
        {:ok, event} -> MapSet.put(normalized, event)
        {:error, _reason} -> normalized
      end
    end)
  end

  def subscribe_wire_events(refcounts, changes) do
    cond do
      Map.has_key?(refcounts, @all_events) -> MapSet.new()
      MapSet.member?(changes, @all_events) -> MapSet.new([@all_events])
      true -> MapSet.reject(changes, &Map.has_key?(refcounts, &1))
    end
  end

  def unsubscribe_wire_events(refcounts, changes) do
    all_count = Map.get(refcounts, @all_events, 0)

    cond do
      all_count > 1 ->
        MapSet.new()

      all_count == 1 and MapSet.member?(changes, @all_events) ->
        unowned_events_after(refcounts, changes)

      all_count == 1 ->
        MapSet.new()

      true ->
        MapSet.filter(changes, &(Map.get(refcounts, &1, 0) == 1))
    end
  end

  def wire_payload(events) do
    if MapSet.member?(events, @all_events) do
      @supported_events
    else
      events |> MapSet.to_list() |> Enum.sort()
    end
  end

  def event_kind(%{"event" => event}), do: normalized_identifier(event)
  def event_kind(%{event: event}), do: normalized_identifier(event)
  def event_kind(%{"kind" => kind}), do: normalized_identifier(kind)
  def event_kind(%{kind: kind}), do: normalized_identifier(kind)
  def event_kind(_value), do: nil

  defp normalized_identifier(event) do
    case EventIdentifier.normalize(event) do
      {:ok, normalized} -> normalized
      {:error, _reason} -> nil
    end
  end

  defp unowned_events_after(refcounts, changes) do
    retained =
      changes
      |> Enum.reduce(refcounts, &decrement_refcount/2)
      |> Map.delete(@all_events)
      |> Map.keys()
      |> MapSet.new()

    MapSet.difference(@supported_event_set, retained)
  end

  defp decrement_refcount(event, refcounts) do
    case Map.get(refcounts, event, 0) do
      count when count <= 1 -> Map.delete(refcounts, event)
      count -> Map.put(refcounts, event, count - 1)
    end
  end
end
