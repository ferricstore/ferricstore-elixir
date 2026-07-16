defmodule FerricStore.SDK.Native.EventCall do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.EventSubscriptions

  @enforce_keys [:id, :action, :subscriber, :events, :opts]
  defstruct [
    :id,
    :action,
    :subscriber,
    :events,
    :opts,
    :from,
    :caller_monitor,
    :queue_timer,
    :request_tag,
    subscriber_reserved: false,
    subscriber_down: false
  ]

  @type action :: :subscribe | :unsubscribe
  @type t :: %__MODULE__{
          id: reference(),
          action: action(),
          subscriber: pid(),
          events: [term()],
          opts: RequestContext.t(),
          from: GenServer.from() | nil,
          caller_monitor: reference() | nil,
          queue_timer: reference() | nil,
          request_tag: reference() | nil,
          subscriber_reserved: boolean(),
          subscriber_down: boolean()
        }

  @type plan ::
          {:noop, MapSet.t()}
          | {:local, MapSet.t()}
          | {:wire, MapSet.t(), MapSet.t()}

  @spec new(action(), pid(), [term()], RequestContext.t(), GenServer.from() | nil) :: t()
  def new(action, subscriber, events, %RequestContext{} = opts, from)
      when action in [:subscribe, :unsubscribe] and is_pid(subscriber) and is_list(events) do
    %__MODULE__{
      id: make_ref(),
      action: action,
      subscriber: subscriber,
      events: events,
      opts: opts,
      from: from,
      caller_monitor: caller_monitor(from)
    }
  end

  @spec subscriber_down(pid(), timeout()) :: t()
  def subscriber_down(subscriber, timeout) when is_pid(subscriber) do
    %__MODULE__{
      id: make_ref(),
      action: :unsubscribe,
      subscriber: subscriber,
      events: [],
      opts: RequestContext.new([timeout: timeout], timeout),
      subscriber_down: true
    }
  end

  @spec plan(t(), EventSubscriptions.t()) :: plan()
  def plan(%__MODULE__{action: :subscribe} = call, subscriptions) do
    %{changes: changes, wire_events: wire_events} =
      EventSubscriptions.plan_subscribe(subscriptions, call.subscriber, call.events)

    plan_result(changes, wire_events)
  end

  def plan(%__MODULE__{action: :unsubscribe} = call, subscriptions) do
    %{changes: changes, wire_events: wire_events} =
      EventSubscriptions.plan_unsubscribe(subscriptions, call.subscriber, call.events)

    plan_result(changes, wire_events)
  end

  @spec queued(t(), reference() | nil) :: t()
  def queued(%__MODULE__{} = call, timer), do: %{call | queue_timer: timer}

  @spec dequeued(t()) :: t()
  def dequeued(%__MODULE__{subscriber_down: true} = call) do
    opts = RequestContext.new(RequestContext.options(call.opts), :infinity)
    %{call | queue_timer: nil, opts: opts}
  end

  def dequeued(%__MODULE__{} = call), do: %{call | queue_timer: nil}

  @spec put_request_tag(t(), reference()) :: t()
  def put_request_tag(%__MODULE__{} = call, tag) when is_reference(tag),
    do: %{call | request_tag: tag}

  @spec reserve_subscriber(t()) :: t()
  def reserve_subscriber(%__MODULE__{} = call), do: %{call | subscriber_reserved: true}

  defp plan_result(changes, wire_events) do
    cond do
      MapSet.size(changes) == 0 -> {:noop, changes}
      MapSet.size(wire_events) == 0 -> {:local, changes}
      true -> {:wire, changes, wire_events}
    end
  end

  defp caller_monitor({caller, _tag}) when is_pid(caller), do: Process.monitor(caller)
  defp caller_monitor(nil), do: nil
end
