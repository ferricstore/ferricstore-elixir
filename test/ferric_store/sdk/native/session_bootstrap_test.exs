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

  defmodule RecordingConnectionStub do
    use GenServer

    def start_link(startup, owner), do: GenServer.start_link(__MODULE__, {startup, owner})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(
          {:request, opcode, payload, _lane, _timeout, _deadline},
          _from,
          {startup, owner} = state
        ) do
      send(owner, {:bootstrap_request, opcode, payload})
      {:reply, {:ok, startup}, state}
    end

    def handle_call({:complete_bootstrap, _startup}, _from, state),
      do: {:reply, :ok, state}
  end

  test "HELLO explicitly requests only the compact result codecs the SDK decodes" do
    {:ok, connection} =
      RecordingConnectionStub.start_link(NativeServer.startup_payload(), self())

    assert {:ok, nil} =
             SessionBootstrap.establish(connection,
               client_name: "codec-test",
               username: nil,
               password: nil,
               topology_endpoint: nil,
               request_timeout: fn -> {:ok, 100} end
             )

    assert_receive {:bootstrap_request, _opcode,
                    %{
                      "compact_flow_responses" => false,
                      "compact_response_codecs" => ["flow_query_result_v1"]
                    }}
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
