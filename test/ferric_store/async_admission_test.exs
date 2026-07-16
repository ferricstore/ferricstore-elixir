defmodule FerricStore.AsyncAdmissionTest do
  use ExUnit.Case, async: true

  alias FerricStore.{AsyncDelivery, AsyncRequest, Client, Error, SDK}
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  test "failed cancellation leaves the async delivery handle active" do
    ref = AsyncDelivery.new()
    request = %AsyncRequest{client: self(), source: self(), owner: self(), ref: ref}

    assert {:error, {:cancel_failed, {:invalid_timeout, -1}}} =
             Client.cancel_async(request, -1)

    assert :ok = AsyncDelivery.deliver(ref, AsyncRequest, {:ok, "late-result"})
    assert {:ok, "late-result"} = Client.yield(request, 50)
  end

  test "await preserves an in-flight terminal result when client DOWN arrives first" do
    request = cross_sender_terminal_request({:ok, "terminal-result"})

    assert Client.await(request, 100) == "terminal-result"
  end

  test "yield preserves an in-flight terminal result when client DOWN arrives first" do
    request = cross_sender_terminal_request({:ok, "terminal-result"})

    assert Client.yield(request, 100) == {:ok, "terminal-result"}
  end

  test "await follows the terminal result producer instead of the client supervisor" do
    client = spawn(fn -> :ok end)
    monitor = Process.monitor(client)
    assert_receive {:DOWN, ^monitor, :process, ^client, _reason}

    ref = AsyncDelivery.new()
    owner = self()

    source =
      spawn(fn ->
        Process.sleep(40)
        AsyncDelivery.deliver(ref, AsyncRequest, {:ok, "terminal-result"})
        send(owner, :source_finished)
      end)

    request = %AsyncRequest{client: client, source: source, owner: self(), ref: ref}

    assert Client.await(request, 200) == "terminal-result"
    assert_receive :source_finished
  end

  test "timed-out async admission is bounded and every returned handle is terminal" do
    {_server, client} = start_sdk(max_pending_requests: 4)
    coordinator = ClientRuntime.coordinator(client)
    :ok = :sys.suspend(coordinator)

    requests =
      Enum.map(1..20, fn index ->
        Client.async_native(
          client,
          :get,
          %{"key" => "queued-#{index}"},
          key: "queued-#{index}",
          timeout: :infinity,
          call_timeout: 0
        )
      end)

    {:message_queue_len, queued} = Process.info(coordinator, :message_queue_len)

    try do
      assert queued <= 4
    after
      :ok = :sys.resume(coordinator)
    end

    results = Enum.map(requests, &Client.await(&1, 1_000))

    assert Enum.all?(results, fn
             {:error, %Error{raw: reason}} when reason in [:timeout, :client_backpressure] -> true
             _other -> false
           end)
  end

  defp start_sdk(opts) do
    {:ok, server} = NativeServer.start_link(owner: self())

    {:ok, client} =
      SDK.start_link(
        Keyword.merge(
          [seeds: [{"127.0.0.1", NativeServer.port(server)}], warm_connections: false],
          opts
        )
      )

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server)
    end)

    {server, client}
  end

  defp cross_sender_terminal_request(result) do
    client = spawn(fn -> :ok end)
    monitor = Process.monitor(client)
    assert_receive {:DOWN, ^monitor, :process, ^client, _reason}

    ref = AsyncDelivery.new()

    source =
      spawn(fn ->
        Process.sleep(5)
        AsyncDelivery.deliver(ref, AsyncRequest, result)
      end)

    %AsyncRequest{client: client, source: source, owner: self(), ref: ref}
  end
end
