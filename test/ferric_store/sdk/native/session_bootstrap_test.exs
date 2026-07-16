defmodule FerricStore.SDK.Native.SessionBootstrapTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.SessionBootstrap
  alias FerricStore.Test.NativeServer

  defmodule ConnectionStub do
    use GenServer

    def start_link(startup), do: GenServer.start_link(__MODULE__, startup)

    @impl true
    def init(startup), do: {:ok, startup}

    @impl true
    def handle_call({:request, _opcode, _payload, _lane, _timeout, _deadline}, _from, startup),
      do: {:reply, {:ok, startup}, startup}

    def handle_call({:complete_bootstrap, _startup}, _from, startup),
      do: {:reply, :ok, startup}
  end

  test "bootstrap cannot report success after its shared deadline expires" do
    {:ok, connection} = ConnectionStub.start_link(NativeServer.startup_payload())
    calls = :atomics.new(1, signed: false)

    request_timeout = fn ->
      if :atomics.add_get(calls, 1, 1) <= 3,
        do: {:ok, 100},
        else: {:error, :timeout}
    end

    assert {:error, :timeout} =
             SessionBootstrap.establish(connection,
               client_name: "deadline-test",
               username: nil,
               password: nil,
               topology_endpoint: nil,
               request_timeout: request_timeout
             )
  end
end
