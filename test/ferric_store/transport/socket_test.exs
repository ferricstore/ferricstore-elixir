defmodule FerricStore.Transport.SocketTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.Socket

  test "TCP connections bound blocked sends and close ambiguous streams on timeout" do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    acceptor = Task.async(fn -> :gen_tcp.accept(listener) end)

    assert {:ok, :gen_tcp, socket} =
             Socket.connect(%{
               host: "127.0.0.1",
               native_port: port,
               send_timeout: 37
             })

    assert {:ok, [send_timeout: 37, send_timeout_close: true]} =
             :inet.getopts(socket, [:send_timeout, :send_timeout_close])

    assert {:ok, accepted} = Task.await(acceptor)
    :ok = :gen_tcp.close(socket)
    :ok = :gen_tcp.close(accepted)
    :ok = :gen_tcp.close(listener)
  end

  test "TCP connections use a finite send timeout by default" do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)

    assert {:ok, :gen_tcp, socket} =
             Socket.connect(%{host: "127.0.0.1", native_port: port})

    assert {:ok, accepted} = :gen_tcp.accept(listener)

    assert {:ok, [send_timeout: 5_000, send_timeout_close: true]} =
             :inet.getopts(socket, [:send_timeout, :send_timeout_close])

    :ok = :gen_tcp.close(socket)
    :ok = :gen_tcp.close(accepted)
    :ok = :gen_tcp.close(listener)
  end

  test "a peer that stops reading cannot block a TCP send indefinitely" do
    {:ok, listener} =
      :gen_tcp.listen(0, [
        :binary,
        active: false,
        packet: :raw,
        reuseaddr: true,
        recbuf: 1_024
      ])

    {:ok, {_address, port}} = :inet.sockname(listener)

    assert {:ok, :gen_tcp, socket} =
             Socket.connect(%{
               host: "127.0.0.1",
               native_port: port,
               send_timeout: 25
             })

    assert {:ok, accepted} = :gen_tcp.accept(listener)
    :ok = :inet.setopts(socket, sndbuf: 1_024)
    payload = :binary.copy(<<0>>, 4 * 1_024 * 1_024)

    result =
      Enum.reduce_while(1..8, :ok, fn _, _acc ->
        case Socket.send(:gen_tcp, socket, payload) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    assert {:error, reason} = result
    assert reason in [:timeout, :closed]
    assert {:error, closed_reason} = Socket.send(:gen_tcp, socket, <<1>>)
    assert closed_reason in [:closed, :enotconn]

    :ok = :gen_tcp.close(accepted)
    :ok = :gen_tcp.close(listener)
  end
end
