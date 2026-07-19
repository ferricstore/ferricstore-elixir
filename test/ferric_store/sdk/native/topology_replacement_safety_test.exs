defmodule FerricStore.SDK.Native.TopologyReplacementSafetyTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "capacity replacement drains a mutation queued behind a failed topology request" do
    assert_queued_mutation_survives_replacement(1)
  end

  test "overlapping replacement drains a mutation queued behind a failed topology request" do
    assert_queued_mutation_survives_replacement(2)
  end

  defp assert_queued_mutation_survives_replacement(max_connections) do
    test_process = self()
    {:ok, port_holder} = Agent.start_link(fn -> nil end)
    {:ok, shard_calls} = Agent.start_link(fn -> 0 end)
    {:ok, commits} = Agent.start_link(fn -> 0 end)

    response_fun = fn
      %{opcode: 0x0007} ->
        call = Agent.get_and_update(shard_calls, &{&1 + 1, &1 + 1})
        topology = NativeServer.topology_payload(Agent.get(port_holder, & &1))

        case call do
          1 ->
            topology

          2 ->
            send(test_process, {:replacement_refresh_waiting, self()})

            receive do
              :release_refresh -> {:reply, "refresh-failed", status: 1}
            end

          _replacement ->
            topology
        end

      %{opcode: 0x0102} ->
        Agent.update(commits, &(&1 + 1))
        {:reply_after, 200, "OK"}

      %{opcode: 0x0001} ->
        %{"protocol" => "ferricstore-native"}

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", port}],
        max_connections: max_connections,
        max_connecting: 1,
        connections_per_endpoint: 1
      )

    on_exit(fn ->
      SDK.close(client)
      stop_server(server)
    end)

    original = only_connection(client)
    refresh = Task.async(fn -> SDK.refresh_topology(client) end)
    assert_receive {:replacement_refresh_waiting, handler}, 500

    write = Task.async(fn -> SDK.set(client, "queued-mutation", "value", timeout: 1_000) end)

    assert_eventually(fn ->
      original
      |> :sys.get_state()
      |> Map.fetch!(:pending)
      |> Enum.any?(fn {_request_id, pending} -> pending.opcode == 0x0102 end)
    end)

    send(handler, :release_refresh)
    assert_receive {:native_server_request, %{opcode: 0x0102}}, 500

    assert Task.await(write, 1_000) == {:ok, :ok}
    assert Task.await(refresh, 1_000) == :ok
    assert Agent.get(commits, & &1) == 1

    replacement = only_connection(client)
    refute replacement == original
    assert Process.alive?(replacement)
    assert_eventually(fn -> not Process.alive?(original) end)
    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
  end

  defp only_connection(client) do
    client
    |> ClientRuntime.state()
    |> Map.fetch!(:connection_pool)
    |> Map.fetch!(:connections)
    |> Map.values()
    |> then(fn [connection] -> connection end)
  end

  defp stop_server(server) do
    if Process.alive?(server), do: GenServer.stop(server, :normal), else: :ok
  catch
    :exit, _reason -> :ok
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())
end
