defmodule FerricStore.SDK.Native.ClientHeartbeatFailureTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.Test.NativeServer

  @tag capture_log: true
  test "a read pending during heartbeat failure retries on a replacement connection" do
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, get_attempts} = Agent.start_link(fn -> 0 end)

    response_fun = fn
      %{opcode: 0x0007} ->
        NativeServer.topology_payload(Agent.get(port_holder, & &1))

      %{opcode: 0x0003} ->
        if Agent.get(get_attempts, & &1) == 1, do: :noreply, else: "OK"

      %{opcode: 0x0101} ->
        case Agent.get_and_update(get_attempts, &{&1 + 1, &1 + 1}) do
          1 -> :noreply
          _retry -> "recovered"
        end

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        heartbeat_interval: 20,
        heartbeat_timeout: 20,
        max_connections: 2,
        connections_per_endpoint: 2
      )

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server, :normal)
    end)

    assert {:ok, "recovered"} = SDK.get(client, "heartbeat-retry", timeout: 1_000)
    assert Agent.get(get_attempts, & &1) == 2
  end
end
