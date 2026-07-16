defmodule FerricStore.SDK.Native.EventSubscriptions do
  @moduledoc false

  alias FerricStore.SDK.Native.{EventSubscriptionRegistry, EventSubscriptionWirePolicy}

  @management_events MapSet.new(["TOPOLOGY_CHANGED"])

  defstruct subscribers: %{},
            refcounts: %{},
            management_events: @management_events,
            connection: nil

  @type t :: %__MODULE__{
          subscribers: %{optional(pid()) => %{monitor: reference(), events: MapSet.t()}},
          refcounts: %{optional(term()) => pos_integer()},
          management_events: MapSet.t(binary()),
          connection: pid() | nil
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec management_events() :: MapSet.t(binary())
  def management_events, do: @management_events

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = subscriptions),
    do: MapSet.size(desired_events(subscriptions)) == 0

  @spec subscribers(t()) :: map()
  def subscribers(%__MODULE__{subscribers: subscribers}), do: subscribers

  @spec subscriber_count(t()) :: non_neg_integer()
  def subscriber_count(%__MODULE__{subscribers: subscribers}), do: map_size(subscribers)

  @spec subscriber(t(), pid()) :: map() | nil
  def subscriber(%__MODULE__{subscribers: subscribers}, pid), do: Map.get(subscribers, pid)

  @spec refcounts(t()) :: map()
  def refcounts(%__MODULE__{refcounts: refcounts}), do: refcounts

  @spec desired_events(t()) :: MapSet.t()
  def desired_events(%__MODULE__{refcounts: refcounts, management_events: management_events}),
    do: refcounts |> Map.keys() |> MapSet.new() |> MapSet.union(management_events)

  @spec connection(t()) :: pid() | nil
  def connection(%__MODULE__{connection: connection}), do: connection

  @spec put_connection(t(), pid() | nil) :: t()
  def put_connection(%__MODULE__{} = subscriptions, connection),
    do: %{subscriptions | connection: connection}

  @spec clear_connection(t(), pid()) :: t()
  def clear_connection(%__MODULE__{connection: connection} = subscriptions, connection),
    do: %{subscriptions | connection: nil}

  def clear_connection(%__MODULE__{} = subscriptions, _connection), do: subscriptions

  @spec normalize([term()]) :: MapSet.t()
  defdelegate normalize(events), to: EventSubscriptionWirePolicy

  @spec subscriber_events(t(), pid()) :: MapSet.t()
  def subscriber_events(%__MODULE__{} = subscriptions, subscriber) do
    case subscriber(subscriptions, subscriber) do
      %{events: events} -> events
      nil -> MapSet.new()
    end
  end

  @spec plan_subscribe(t(), pid(), [term()]) :: %{changes: MapSet.t(), wire_events: MapSet.t()}
  def plan_subscribe(%__MODULE__{} = subscriptions, subscriber, requested) do
    changes =
      requested
      |> normalize()
      |> MapSet.difference(subscriber_events(subscriptions, subscriber))

    %{changes: changes, wire_events: subscribe_wire_events(subscriptions, changes)}
  end

  @spec plan_unsubscribe(t(), pid(), [term()]) :: %{changes: MapSet.t(), wire_events: MapSet.t()}
  def plan_unsubscribe(%__MODULE__{} = subscriptions, subscriber, requested) do
    current = subscriber_events(subscriptions, subscriber)

    changes =
      case requested do
        [] -> current
        events -> MapSet.intersection(normalize(events), current)
      end

    %{changes: changes, wire_events: unsubscribe_wire_events(subscriptions, changes)}
  end

  @spec subscribe_wire_events(t(), MapSet.t()) :: MapSet.t()
  def subscribe_wire_events(%__MODULE__{} = subscriptions, changes),
    do:
      EventSubscriptionWirePolicy.subscribe_wire_events(
        effective_refcounts(subscriptions),
        changes
      )

  @spec unsubscribe_wire_events(t(), MapSet.t()) :: MapSet.t()
  def unsubscribe_wire_events(%__MODULE__{} = subscriptions, changes),
    do:
      EventSubscriptionWirePolicy.unsubscribe_wire_events(
        effective_refcounts(subscriptions),
        changes
      )

  @spec subscribe(t(), pid(), MapSet.t(), pid() | nil) :: t()
  defdelegate subscribe(subscriptions, subscriber, events, connection),
    to: EventSubscriptionRegistry

  @spec unsubscribe(t(), pid(), MapSet.t()) :: t()
  defdelegate unsubscribe(subscriptions, subscriber, events), to: EventSubscriptionRegistry

  @spec wire_payload(MapSet.t()) :: [binary()]
  defdelegate wire_payload(events), to: EventSubscriptionWirePolicy

  @spec event_kind(term()) :: term() | nil
  defdelegate event_kind(event), to: EventSubscriptionWirePolicy

  defp effective_refcounts(%__MODULE__{} = subscriptions) do
    Enum.reduce(subscriptions.management_events, subscriptions.refcounts, fn event, refcounts ->
      Map.update(refcounts, event, 1, &(&1 + 1))
    end)
  end
end
