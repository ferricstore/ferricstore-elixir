defmodule FerricStore.SDK.Native.RefreshOperation do
  @moduledoc false

  @enforce_keys [:refresher, :monitor, :token]
  defstruct [
    :refresher,
    :monitor,
    :token,
    :connection_reservation,
    waiters: [],
    waiter_count: 0,
    cancelled_waiters: MapSet.new(),
    waiter_keys: MapSet.new()
  ]

  @type waiter ::
          {:refresh_call, GenServer.from(), reference(), reference() | nil,
           FerricStore.RequestContext.t()}
          | {:request_retry, reference()}
          | {:batch_retry, reference()}
          | :topology_event
          | :topology_event_followup

  @type t :: %__MODULE__{
          refresher: pid(),
          monitor: reference(),
          token: reference(),
          connection_reservation: boolean(),
          waiters: [waiter()],
          waiter_count: non_neg_integer(),
          cancelled_waiters: MapSet.t(term()),
          waiter_keys: MapSet.t(term())
        }

  @spec new(pid(), reference(), reference(), boolean()) :: t()
  def new(refresher, monitor, token, connection_reservation),
    do: %__MODULE__{
      refresher: refresher,
      monitor: monitor,
      token: token,
      connection_reservation: connection_reservation
    }

  @spec add(t(), waiter()) :: {t(), boolean()}
  def add(%__MODULE__{} = operation, waiter) do
    key = waiter_key(waiter)

    if MapSet.member?(operation.waiter_keys, key) do
      {operation, false}
    else
      {%{
         operation
         | waiters: [waiter | operation.waiters],
           waiter_count: operation.waiter_count + 1,
           waiter_keys: MapSet.put(operation.waiter_keys, key),
           cancelled_waiters: MapSet.delete(operation.cancelled_waiters, key)
       }, true}
    end
  end

  @spec cancel(t(), term()) :: :missing | :empty | {:ok, t()}
  def cancel(%__MODULE__{} = operation, key) do
    if MapSet.member?(operation.waiter_keys, key) do
      if operation.waiter_count <= 1 do
        :empty
      else
        {:ok,
         %{
           operation
           | waiter_count: operation.waiter_count - 1,
             waiter_keys: MapSet.delete(operation.waiter_keys, key),
             cancelled_waiters: MapSet.put(operation.cancelled_waiters, key)
         }}
      end
    else
      :missing
    end
  end

  @spec active_waiters(t()) :: [waiter()]
  def active_waiters(%__MODULE__{} = operation) do
    Enum.reject(operation.waiters, fn waiter ->
      MapSet.member?(operation.cancelled_waiters, waiter_key(waiter))
    end)
  end

  @spec waiter_key(waiter()) :: term()
  def waiter_key({:refresh_call, _from, monitor, _timer, _context}),
    do: {:refresh_call, monitor}

  def waiter_key({:request_retry, tag}), do: {:request_retry, tag}
  def waiter_key({:batch_retry, batch_id}), do: {:batch_retry, batch_id}
  def waiter_key(waiter) when waiter in [:topology_event, :topology_event_followup], do: waiter
end
