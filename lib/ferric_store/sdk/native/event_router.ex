defmodule FerricStore.SDK.Native.EventRouter do
  @moduledoc false

  @all_events {:ferricstore, :all_events}

  @spec all_events() :: {:ferricstore, :all_events}
  def all_events, do: @all_events

  @spec deliver(pid(), map(), map(), binary() | nil | :broadcast) :: :ok
  def deliver(client, subscribers, event, :broadcast) do
    Enum.each(subscribers, fn {subscriber, _subscription} ->
      send(subscriber, {:ferricstore_event, client, event})
    end)
  end

  def deliver(client, subscribers, event, kind) do
    Enum.each(subscribers, fn {subscriber, %{events: events}} ->
      if MapSet.member?(events, @all_events) or
           (not is_nil(kind) and MapSet.member?(events, kind)) do
        send(subscriber, {:ferricstore_event, client, event})
      end
    end)

    :ok
  end
end
