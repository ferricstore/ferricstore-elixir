defmodule FerricStore.SDK.Native.EventInboxTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.Test.ClientRuntime

  test "an in-flight event wins when the client DOWN signal arrives first" do
    coordinator = spawn(fn -> receive do: (:stop -> :ok) end)
    event_source = spawn(fn -> event_source_loop() end)
    on_exit(fn -> if Process.alive?(coordinator), do: send(coordinator, :stop) end)

    on_exit(fn ->
      if Process.alive?(event_source), do: send(event_source, :stop)
    end)

    {:ok, client} = ClientRuntime.wrap({:ok, coordinator}, event_source: event_source)
    Process.unlink(client)
    owner = self()
    event = %{opcode: 0x000A, value: %{"flow_id" => "flow-1"}}

    spawn(fn ->
      await_monitor(owner, client, event_source)
      Process.exit(client, :kill)
      await_exit(client)
      Process.sleep(40)
      send(event_source, {:deliver, owner, client, event})
    end)

    assert SDK.await_event(client, 500) == {:ok, event}
  end

  defp event_source_loop do
    receive do
      {:deliver, subscriber, client, event} ->
        send(subscriber, {:ferricstore_event, client, event})

      :stop ->
        :ok
    end
  end

  defp await_monitor(owner, client, event_source) do
    client_monitors = Process.info(client, :monitored_by) |> elem(1)
    source_monitors = Process.info(event_source, :monitored_by) |> elem(1)

    if owner in client_monitors or owner in source_monitors do
      :ok
    else
      Process.sleep(1)
      await_monitor(owner, client, event_source)
    end
  end

  defp await_exit(client) do
    if Process.alive?(client) do
      Process.sleep(1)
      await_exit(client)
    else
      :ok
    end
  end
end
