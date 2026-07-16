defmodule FerricStore.Flow.QueryOptionContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    def start_link(owner) do
      GenServer.start_link(__MODULE__, owner)
      |> ClientRuntime.wrap()
    end

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, owner) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, owner)
    end

    def handle_call({:request, _opcode, payload, _context}, _from, owner) do
      send(owner, {:flow_payload, payload})
      {:reply, {:ok, []}, owner}
    end
  end

  test "list rejects unsupported return modes before transport" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :list, :return, :expected_meta_return}
            }} = Flow.list(client, type: "email", return: :records)

    refute_received {:flow_payload, _payload}
  end

  test "list accepts the canonical meta return mode case-insensitively" do
    for return <- [:meta, "meta", "MeTa"] do
      {:ok, client} = CaptureClient.start_link(self())

      assert [] = Flow.list(client, type: "email", return: return)
      assert_received {:flow_payload, %{"return" => ^return}}
    end
  end
end
