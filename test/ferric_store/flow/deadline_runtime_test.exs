defmodule FerricStore.Flow.DeadlineRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.SDK.Native.AdmissionGate
  alias FerricStore.Test.ClientRuntime

  defmodule SlowDecodeCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode(value), do: value

    @impl true
    def decode(value) do
      Process.sleep(100)
      value
    end
  end

  defmodule ReplyClient do
    use GenServer

    def start_link(response),
      do: GenServer.start_link(__MODULE__, response) |> ClientRuntime.wrap()

    @impl true
    def init(response), do: {:ok, response}

    @impl true
    def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, response) do
      :ok = AdmissionGate.release(gate)
      handle_call(request, from, response)
    end

    def handle_call({:command, _opcode, _key, _payload, _context}, _from, response),
      do: {:reply, {:ok, response}, response}
  end

  test "response codec work cannot turn an expired absolute deadline into success" do
    {:ok, client} = ReplyClient.start_link(%{"payload" => "encoded"})

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.get(client, "flow", codec: SlowDecodeCodec, timeout: 50)
  end
end
