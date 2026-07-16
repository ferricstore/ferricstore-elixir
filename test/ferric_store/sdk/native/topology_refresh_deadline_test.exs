defmodule FerricStore.SDK.Native.TopologyRefreshDeadlineTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext
  alias FerricStore.SDK
  alias FerricStore.SDK.Native.Coordinator.State
  alias FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "a queued refresh completion cannot beat an expired waiter deadline" do
    reply_tag = make_ref()
    monitor = Process.monitor(self())
    context = RequestContext.new([timeout: 0], 100)

    state =
      %State{}
      |> State.put_lifecycle_monitor(monitor, {:refresh_waiter, monitor})
      |> State.adjust_refresh_calls(1)

    state =
      CoordinatorTopologyRefreshRuntime.finish_waiter(
        {:refresh_call, {self(), reply_tag}, monitor, nil, context},
        :ok,
        state,
        %{}
      )

    assert_receive {^reply_tag, {:error, :timeout}}
    assert state.topology_manager.refresh_calls == 0
  end

  test "caller deadline releases an unfinished topology refresh immediately" do
    {:ok, topology_requests} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn
      %{opcode: 0x0007} ->
        request = Agent.get_and_update(topology_requests, &{&1, &1 + 1})

        if request == 0,
          do: NativeServer.topology_payload(Agent.get(port_holder, & &1)),
          else: :noreply

      _request ->
        "OK"
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _port -> NativeServer.port(server) end)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        max_pending_requests: 1,
        topology_refresh_timeout: 1_000
      )

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server)
    end)

    assert {:error, :timeout} = SDK.refresh_topology(client, 20)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      state.topology_manager.refresh_operation == nil and
        state.topology_manager.refresh_calls == 0
    end)

    assert {:error, :timeout} = SDK.refresh_topology(client, 20)
  end

  defp assert_eventually(predicate, attempts \\ 25)

  defp assert_eventually(predicate, attempts) when attempts > 0 do
    if predicate.() do
      assert true
    else
      Process.sleep(10)
      assert_eventually(predicate, attempts - 1)
    end
  end

  defp assert_eventually(predicate, 0), do: assert(predicate.())
end
