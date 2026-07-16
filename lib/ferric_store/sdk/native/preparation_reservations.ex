defmodule FerricStore.SDK.Native.PreparationReservations do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.CoordinatorTimers

  defmodule Entry do
    @moduledoc false

    @enforce_keys [:owner, :monitor, :item_count]
    defstruct [:owner, :monitor, :timer, :item_count]

    @type t :: %__MODULE__{
            owner: pid(),
            monitor: reference(),
            timer: reference() | nil,
            item_count: non_neg_integer()
          }
  end

  defstruct entries: %{}

  @type token :: reference()
  @type t :: %__MODULE__{entries: %{optional(token()) => Entry.t()}}

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{entries: entries}), do: map_size(entries)

  @spec reserve(t(), pid(), non_neg_integer(), RequestContext.t()) ::
          {token(), Entry.t(), t()}
  def reserve(%__MODULE__{} = reservations, owner, item_count, %RequestContext{} = context)
      when is_pid(owner) and is_integer(item_count) and item_count >= 0 do
    token = make_ref()
    monitor = Process.monitor(owner)
    timer = reservation_timer(token, RequestContext.remaining(context))
    entry = %Entry{owner: owner, monitor: monitor, timer: timer, item_count: item_count}

    {token, entry, %{reservations | entries: Map.put(reservations.entries, token, entry)}}
  end

  @spec fetch(t(), token()) :: {:ok, Entry.t()} | :error
  def fetch(%__MODULE__{entries: entries}, token) when is_reference(token),
    do: Map.fetch(entries, token)

  @spec fetch!(t(), token()) :: Entry.t()
  def fetch!(%__MODULE__{} = reservations, token) do
    {:ok, entry} = fetch(reservations, token)
    entry
  end

  @spec take(t(), token()) :: {Entry.t() | nil, t()}
  def take(%__MODULE__{} = reservations, token) when is_reference(token) do
    case Map.pop(reservations.entries, token) do
      {nil, _entries} ->
        {nil, reservations}

      {%Entry{} = entry, entries} ->
        cleanup(entry)
        {entry, %{reservations | entries: entries}}
    end
  end

  defp reservation_timer(_token, :infinity), do: nil

  defp reservation_timer(token, timeout) when is_integer(timeout) and timeout >= 0,
    do: Process.send_after(self(), {:preparation_reservation_timeout, token}, timeout)

  defp cleanup(%Entry{monitor: monitor, timer: timer}) do
    CoordinatorTimers.cancel(timer)
    CoordinatorTimers.demonitor(monitor)
  end
end
