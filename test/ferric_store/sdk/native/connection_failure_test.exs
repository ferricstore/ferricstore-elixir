defmodule FerricStore.SDK.Native.ConnectionFailureTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  @op_set 0x0102
  @op_mset 0x0105
  @op_subscribe_events 0x0011

  test "an abruptly killed connection fails its active scalar request" do
    {_server, client} = start_sdk_with_blocked_opcode(@op_set)

    request =
      Task.async(fn ->
        SDK.set(client, "blocked-write", "value", timeout: :infinity, call_timeout: :infinity)
      end)

    assert_receive {:native_server_request, %{opcode: @op_set}}, 500
    connection = only_pending_connection(client)
    Process.exit(connection, :kill)

    assert {:error, {:transport_failed, {:connection_down, :killed}}} =
             Task.await(request, 1_000)

    assert ClientRuntime.state(client).request_registry.requests == %{}
    assert Process.alive?(client)
  end

  test "an abruptly killed connection completes an active batch" do
    {_server, client} = start_sdk_with_blocked_opcode(@op_mset)

    request =
      Task.async(fn ->
        SDK.mset(client, [{"blocked-batch", "value"}],
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_receive {:native_server_request, %{opcode: @op_mset}}, 500
    connection = only_pending_connection(client)
    Process.exit(connection, :kill)

    assert {:error, {:group_failure, %{failures: [failure]}}} = Task.await(request, 1_000)
    assert failure.reason == {:transport_failed, {:connection_down, :killed}}

    state = ClientRuntime.state(client)
    assert state.request_registry.requests == %{}
    assert state.batch_scheduler.batches == %{}
    assert Process.alive?(client)
  end

  test "an abruptly killed event connection completes the active subscription call" do
    {_server, client} = start_sdk_with_blocked_opcode(@op_subscribe_events)

    request =
      Task.async(fn ->
        SDK.subscribe_events(client, ["flow_wake"],
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_receive {:native_server_request,
                    %{
                      opcode: @op_subscribe_events,
                      payload: %{"events" => ["FLOW_WAKE"]}
                    }},
                   500

    connection = only_pending_connection(client)
    Process.exit(connection, :kill)

    assert {:error, {:transport_failed, {:connection_down, :killed}}} =
             Task.await(request, 1_000)

    state = ClientRuntime.state(client)
    assert state.request_registry.requests == %{}
    assert state.event_coordinator.operation == nil
    assert Process.alive?(client)
  end

  defp start_sdk_with_blocked_opcode(blocked_opcode) do
    response_fun = fn request ->
      case request.opcode do
        0x000C ->
          NativeServer.startup_payload()

        0x0007 ->
          {:ok, {_address, port}} = :inet.sockname(request.socket)
          NativeServer.topology_payload(port)

        ^blocked_opcode ->
          blocked_response(request, blocked_opcode)

        _other ->
          "OK"
      end
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)

    {:ok, client} =
      SDK.start_link(
        seeds: [{"127.0.0.1", NativeServer.port(server)}],
        warm_connections: false
      )

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server)
    end)

    {server, client}
  end

  defp blocked_request?(%{payload: %{"events" => ["TOPOLOGY_CHANGED"]}}, @op_subscribe_events),
    do: false

  defp blocked_request?(_request, _blocked_opcode), do: true

  defp blocked_response(request, blocked_opcode) do
    if blocked_request?(request, blocked_opcode), do: :noreply, else: "OK"
  end

  defp only_pending_connection(client) do
    [{_tag, request}] = Map.to_list(ClientRuntime.state(client).request_registry.requests)
    request.conn
  end
end
