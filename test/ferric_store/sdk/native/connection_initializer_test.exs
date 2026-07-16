defmodule FerricStore.SDK.Native.ConnectionInitializerTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{Connection, ConnectionInitializer}

  defmodule FailingSocket do
    def set_active_once(:test, _socket), do: {:error, :active_once_failed}

    def close(:test, owner) do
      send(owner, :socket_closed)
      :ok
    end
  end

  defmodule UnexpectedEncoder do
    def start(owner) do
      send(owner, :encoder_started)
      self()
    end
  end

  test "activation closes a connected socket when active-once setup fails" do
    state = %Connection{transport: :test, socket: self()}

    assert {:stop, :active_once_failed} =
             ConnectionInitializer.activate(state, FailingSocket, UnexpectedEncoder)

    assert_received :socket_closed
    refute_received :encoder_started
  end
end
