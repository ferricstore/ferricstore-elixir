defmodule FerricStore.SDK.Native.ClientCancellationTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "a successfully cancelled encoded write never reaches the wire" do
    {client, connection, encoder} = start_client_with_suspended_data_encoder()

    request =
      FerricStore.async_native(
        client,
        Protocol.opcode(:set),
        %{"key" => "cancel-before-send", "value" => "value"},
        timeout: :infinity,
        call_timeout: :infinity
      )

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    assert :ok = FerricStore.cancel_async(request)
    assert_eventually(fn -> :sys.get_state(connection).pending == %{} end)

    true = :erlang.resume_process(encoder)

    refute_receive {:native_server_request,
                    %{opcode: 0x0102, payload: %{"key" => "cancel-before-send"}}},
                   200
  end

  test "a caller death cancels a write that has not been authorized for send" do
    {client, connection, encoder} = start_client_with_suspended_data_encoder()

    caller =
      spawn(fn ->
        FerricStore.SDK.set(client, "dead-caller-write", "value",
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    Process.exit(caller, :kill)

    assert_eventually(fn ->
      ClientRuntime.state(client).request_registry.requests == %{} and
        :sys.get_state(connection).pending == %{}
    end)

    true = :erlang.resume_process(encoder)

    refute_receive {:native_server_request,
                    %{opcode: 0x0102, payload: %{"key" => "dead-caller-write"}}},
                   200
  end

  test "a caller death cancels a batch write that has not been authorized for send" do
    {client, connection, encoder} = start_client_with_suspended_data_encoder()

    caller =
      spawn(fn ->
        FerricStore.SDK.mset(client, [{"dead-batch-write", "value"}],
          timeout: :infinity,
          call_timeout: :infinity
        )
      end)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    Process.exit(caller, :kill)

    assert_eventually(fn ->
      state = ClientRuntime.state(client)

      state.batch_scheduler.batches == %{} and state.request_registry.requests == %{} and
        :sys.get_state(connection).pending == %{}
    end)

    true = :erlang.resume_process(encoder)

    refute_receive {:native_server_request,
                    %{
                      opcode: 0x0105,
                      payload: %{"pairs" => [%{"key" => "dead-batch-write"}]}
                    }},
                   200
  end

  defp start_client_with_suspended_data_encoder do
    {:ok, server} = NativeServer.start_link(owner: self())
    {:ok, client} = FerricStore.start_link(seeds: [{"127.0.0.1", NativeServer.port(server)}])
    on_exit(fn -> FerricStore.close(client) end)
    flush_server_requests()

    connection = only_connection(client)
    encoder = :sys.get_state(connection).encoder.data
    true = :erlang.suspend_process(encoder)
    on_exit(fn -> resume_if_suspended(encoder) end)
    {client, connection, encoder}
  end

  defp only_connection(client) do
    [connection] =
      client
      |> ClientRuntime.state()
      |> Map.fetch!(:connection_pool)
      |> then(&Map.values(&1.connections))

    connection
  end

  defp flush_server_requests do
    receive do
      {:native_server_request, _request} -> flush_server_requests()
      {:native_server_connected, _handler} -> flush_server_requests()
    after
      0 -> :ok
    end
  end

  defp resume_if_suspended(process) do
    if Process.alive?(process) and Process.info(process, :status) == {:status, :suspended} do
      true = :erlang.resume_process(process)
    end

    :ok
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
