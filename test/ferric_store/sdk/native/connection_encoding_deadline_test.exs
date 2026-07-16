defmodule FerricStore.SDK.Native.ConnectionEncodingDeadlineTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.Connection
  alias FerricStore.Test.NativeServer

  test "expired large data encoding cannot block a later small data request" do
    {:ok, server} = NativeServer.start_link(owner: self())

    endpoint = %{
      host: "127.0.0.1",
      native_port: NativeServer.port(server),
      max_request_bytes: 32 * 1_024 * 1_024
    }

    {:ok, connection} = Connection.start(endpoint)

    on_exit(fn ->
      if Process.alive?(connection), do: Connection.close(connection)
      if Process.alive?(server), do: GenServer.stop(server, :normal)
    end)

    items =
      Enum.map(1..100_000, fn index ->
        %{
          "a" => index,
          "b" => "value-value-value-value-value-value-value-value",
          "c" => [index, index, index, index],
          "d" => %{"index" => index, "values" => [index, index]}
        }
      end)

    tag = make_ref()
    data_encoder = :sys.get_state(connection).encoder.data
    {:reductions, before_encoding} = Process.info(data_encoder, :reductions)

    assert :ok =
             Connection.async_request(
               connection,
               self(),
               tag,
               0x0101,
               %{"items" => items},
               1,
               150
             )

    assert_eventually(fn ->
      {:reductions, reductions} = Process.info(data_encoder, :reductions)
      reductions - before_encoding > 10_000
    end)

    assert {:ok, "OK"} =
             Connection.request(connection, 0x0101, %{"key" => "small"}, 1, 250)

    assert_receive {:ferricstore_connection_response, ^connection, ^tag, {:error, :timeout}},
                   500

    refute_receive {:native_server_request, %{payload: %{"items" => _items}}}, 100
  end

  defp assert_eventually(fun, attempts \\ 100)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(2)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())
end
