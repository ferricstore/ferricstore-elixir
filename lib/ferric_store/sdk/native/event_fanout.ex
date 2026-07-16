defmodule FerricStore.SDK.Native.EventFanout do
  @moduledoc false

  use GenServer

  alias FerricStore.SDK.Native.{EventRouter, EventSubscriptions}

  @default_max_queue 1_024

  @enforce_keys [:pid, :pending, :max_queue, :delivery_token]
  defstruct [:pid, :pending, :max_queue, :delivery_token]

  @type t :: %__MODULE__{
          pid: pid(),
          pending: :atomics.atomics_ref(),
          max_queue: pos_integer(),
          delivery_token: reference()
        }
  @type dispatch_result :: :ok | :dropped

  @spec start_link(pid(), keyword()) :: {:ok, t()} | {:error, term()}
  def start_link(owner, opts \\ []) when is_pid(owner) and is_list(opts) do
    pending = :atomics.new(1, signed: false)
    delivery_token = make_ref()
    max_queue = positive_option(opts, :max_queue, @default_max_queue)

    case start_supervised({owner, pending, delivery_token}) do
      {:ok, pid} -> {:ok, handle(pid, pending, max_queue, delivery_token)}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec start_supervised({pid(), :atomics.atomics_ref(), reference()}) :: GenServer.on_start()
  def start_supervised({owner, pending, delivery_token})
      when is_pid(owner) and is_reference(delivery_token),
      do: GenServer.start_link(__MODULE__, {owner, pending, delivery_token})

  @doc false
  @spec handle(pid(), :atomics.atomics_ref(), pos_integer(), reference()) :: t()
  def handle(pid, pending, max_queue, delivery_token)
      when is_pid(pid) and is_integer(max_queue) and max_queue > 0 and
             is_reference(delivery_token),
      do: %__MODULE__{
        pid: pid,
        pending: pending,
        max_queue: max_queue,
        delivery_token: delivery_token
      }

  @spec subscribe(t(), pid(), MapSet.t()) :: :ok
  def subscribe(%__MODULE__{pid: pid, delivery_token: token}, subscriber, events)
      when is_pid(subscriber) and is_struct(events, MapSet) do
    send(pid, {__MODULE__, :subscribe, token, subscriber, events})
    :ok
  end

  @spec unsubscribe(t(), pid(), MapSet.t()) :: :ok
  def unsubscribe(%__MODULE__{pid: pid, delivery_token: token}, subscriber, events)
      when is_pid(subscriber) and is_struct(events, MapSet) do
    send(pid, {__MODULE__, :unsubscribe, token, subscriber, events})
    :ok
  end

  @spec remove_subscriber(t(), pid()) :: :ok
  def remove_subscriber(%__MODULE__{pid: pid, delivery_token: token}, subscriber)
      when is_pid(subscriber) do
    send(pid, {__MODULE__, :remove_subscriber, token, subscriber})
    :ok
  end

  @spec dispatch(t(), map(), :broadcast | :by_kind | term()) :: dispatch_result()
  def dispatch(%__MODULE__{} = fanout, event, mode) when is_map(event) do
    pending = :atomics.add_get(fanout.pending, 1, 1)

    if pending <= fanout.max_queue and Process.alive?(fanout.pid) do
      send(fanout.pid, {__MODULE__, :deliver, fanout.delivery_token, event, mode})
      :ok
    else
      :atomics.sub(fanout.pending, 1, 1)
      :dropped
    end
  end

  @spec pending(t()) :: non_neg_integer()
  def pending(%__MODULE__{pending: pending}), do: :atomics.get(pending, 1)

  @doc false
  @spec sync(t()) :: :ok
  def sync(%__MODULE__{pid: pid}), do: GenServer.call(pid, :sync)

  @spec stop(t()) :: :ok
  def stop(%__MODULE__{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal), else: :ok
  catch
    :exit, _reason -> :ok
  end

  @impl true
  def init({owner, pending, delivery_token}) do
    {:ok,
     %{
       owner: owner,
       owner_monitor: Process.monitor(owner),
       pending: pending,
       delivery_token: delivery_token,
       subscribers: %{}
     }}
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_info(
        {__MODULE__, :subscribe, token, subscriber, events},
        %{delivery_token: token} = state
      ) do
    subscribers =
      Map.update(state.subscribers, subscriber, %{events: events}, fn record ->
        %{record | events: MapSet.union(record.events, events)}
      end)

    {:noreply, %{state | subscribers: subscribers}}
  end

  def handle_info(
        {__MODULE__, :unsubscribe, token, subscriber, events},
        %{delivery_token: token} = state
      ) do
    subscribers =
      case Map.fetch(state.subscribers, subscriber) do
        :error ->
          state.subscribers

        {:ok, record} ->
          remaining = MapSet.difference(record.events, events)

          if MapSet.size(remaining) == 0,
            do: Map.delete(state.subscribers, subscriber),
            else: Map.put(state.subscribers, subscriber, %{record | events: remaining})
      end

    {:noreply, %{state | subscribers: subscribers}}
  end

  def handle_info(
        {__MODULE__, :remove_subscriber, token, subscriber},
        %{delivery_token: token} = state
      ) do
    {:noreply, %{state | subscribers: Map.delete(state.subscribers, subscriber)}}
  end

  def handle_info({__MODULE__, :deliver, token, event, mode}, %{delivery_token: token} = state) do
    try do
      EventRouter.deliver(state.owner, state.subscribers, event, delivery_kind(mode, event))
    after
      :atomics.sub(state.pending, 1, 1)
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, monitor, :process, owner, _reason}, state)
      when monitor == state.owner_monitor and owner == state.owner,
      do: {:stop, :normal, state}

  def handle_info(_message, state), do: {:noreply, state}

  defp delivery_kind(:by_kind, event), do: EventSubscriptions.event_kind(event.value)
  defp delivery_kind(mode, _event), do: mode

  defp positive_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end
end
