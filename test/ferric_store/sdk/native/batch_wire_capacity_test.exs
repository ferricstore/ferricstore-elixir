defmodule FerricStore.SDK.Native.BatchWireCapacityTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.SDK.Native.{BatchScheduler, Coordinator.State, Topology}
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "a batch resumes after another batch releases the global wire slots" do
    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)
    waiting_key = "waiting-after-batch-capacity"
    occupying_values = ["value:#{first_key}", "value:#{second_key}"]
    waiting_values = ["value:#{waiting_key}"]
    occupying_keys = MapSet.new([first_key, second_key])
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        port = Agent.get(port_holder, & &1)
        two_shard_topology(port)

      %{opcode: 0x0104, payload: %{"keys" => [key]}} ->
        value = ["value:#{key}"]
        if MapSet.member?(occupying_keys, key), do: {:reply_after, 150, value}, else: value

      _request ->
        "OK"
    end

    {client, _server} = start_client(port_holder, response_fun, max_pending_requests: 2)

    occupying =
      Task.async(fn ->
        SDK.mget(client, [first_key, second_key], timeout: 1_000, max_group_concurrency: 2)
      end)

    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500
    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500

    waiting = Task.async(fn -> SDK.mget(client, [waiting_key], timeout: 750) end)

    assert {:ok, ^occupying_values} = Task.await(occupying, 1_000)
    assert {:ok, ^waiting_values} = Task.await(waiting, 500)
  end

  test "cancelling an event connection attempt resumes a batch waiting for a wire slot" do
    first_key = key_in_slots(0..511)
    second_key = key_in_slots(512..1023)
    waiting_key = "waiting-after-event-connect-cancel"
    waiting_values = ["value:#{waiting_key}"]
    occupying_keys = MapSet.new([first_key, second_key])
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        port = Agent.get(port_holder, & &1)
        two_shard_topology(port)

      %{opcode: 0x0104, payload: %{"keys" => [key]}} ->
        if MapSet.member?(occupying_keys, key), do: :noreply, else: ["value:#{key}"]

      _request ->
        "OK"
    end

    {client, _server} = start_client(port_holder, response_fun, max_pending_requests: 3)
    {:ok, slow_server} = NativeServer.start_link(owner: self(), response_fun: &slow_startup/1)
    flush_server_messages()

    :sys.replace_state(ClientRuntime.coordinator(client), fn state ->
      State.put_event_connection(state, nil)
    end)

    occupying =
      spawn(fn ->
        SDK.mget(client, [first_key, second_key],
          timeout: :infinity,
          call_timeout: :infinity,
          max_group_concurrency: 2
        )
      end)

    on_exit(fn -> if Process.alive?(occupying), do: Process.exit(occupying, :kill) end)
    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500
    assert_receive {:native_server_request, %{opcode: 0x0104}}, 500

    event_caller =
      spawn(fn ->
        SDK.subscribe_events(client, ["flow_wake"],
          endpoint: %{host: "127.0.0.1", native_port: NativeServer.port(slow_server)},
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    on_exit(fn -> if Process.alive?(event_caller), do: Process.exit(event_caller, :kill) end)
    assert_receive {:native_server_request, %{opcode: 0x000C}}, 500

    waiting = Task.async(fn -> SDK.mget(client, [waiting_key], timeout: 750) end)

    assert_eventually(fn ->
      client
      |> ClientRuntime.state()
      |> Map.fetch!(:batch_scheduler)
      |> BatchScheduler.wire_waiting_size()
      |> Kernel.==(1)
    end)

    Process.exit(event_caller, :kill)

    assert_receive {:native_server_request,
                    %{opcode: 0x0104, payload: %{"keys" => [^waiting_key]}}},
                   500

    assert {:ok, ^waiting_values} = Task.await(waiting, 500)
  end

  defp start_client(port_holder, response_fun, opts) do
    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    port = NativeServer.port(server)
    Agent.update(port_holder, fn _ -> port end)
    {:ok, client} = SDK.start_link(Keyword.put(opts, :seeds, [{"127.0.0.1", port}]))
    on_exit(fn -> SDK.close(client) end)
    {client, server}
  end

  defp slow_startup(%{opcode: 0x000C}), do: :noreply
  defp slow_startup(_request), do: "OK"

  defp two_shard_topology(port) do
    %{
      "route_epoch" => 1,
      "shard_count" => 2,
      "ranges" => [
        range(0, 511, 0, 1, port),
        range(512, 1023, 1, 2, port)
      ]
    }
  end

  defp range(first, last, shard, lane_id, port) do
    %{
      "first_slot" => first,
      "last_slot" => last,
      "shard" => shard,
      "lane_id" => lane_id,
      "endpoint" => %{
        "node" => "node-#{shard}",
        "host" => "127.0.0.1",
        "native_port" => port
      }
    }
  end

  defp key_in_slots(range) do
    Enum.find_value(1..10_000, fn index ->
      key = "wire-capacity-key-#{index}"
      if Topology.slot_for_key(key) in range, do: key
    end)
  end

  defp flush_server_messages do
    receive do
      {:native_server_connected, _handler} -> flush_server_messages()
      {:native_server_request, _request} -> flush_server_messages()
      {:native_server_disconnected, _handler, _reason} -> flush_server_messages()
    after
      0 -> :ok
    end
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      receive do
      after
        2 -> assert_eventually(fun, attempts - 1)
      end
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())
end
