defmodule FerricStore.Flow.BatchResponseTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule ReplyClient do
    use GenServer

    def start_link(response) do
      GenServer.start_link(__MODULE__, response)
      |> ClientRuntime.wrap()
    end

    @impl true
    def init(response), do: {:ok, response}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, response) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, response)
    end

    def handle_call({:request, _opcode, _payload, _context}, _from, response),
      do: {:reply, {:ok, response}, response}

    def handle_call({:command, _opcode, _key, _payload, _context}, _from, response),
      do: {:reply, {:ok, response}, response}
  end

  test "create_many decodes returned records with the selected codec" do
    response = [
      %{
        "id" => "flow-1",
        "payload" => Term.encode(%{step: 1}),
        "values" => %{"invoice" => Term.encode(%{total: 120})}
      }
    ]

    {:ok, client} = ReplyClient.start_link(response)

    assert [
             %{
               "id" => "flow-1",
               "payload" => %{step: 1},
               "values" => %{"invoice" => %{total: 120}}
             }
           ] = Flow.create_many(client, ["flow-1"], type: "email", codec: Term)
  end

  test "complete_many decodes returned records with the selected codec" do
    response = [
      %{
        "id" => "flow-1",
        "result" => Term.encode(false),
        "payload" => Term.encode(%{step: 2})
      }
    ]

    {:ok, client} = ReplyClient.start_link(response)

    assert [
             %{"id" => "flow-1", "result" => false, "payload" => %{step: 2}}
           ] =
             Flow.complete_many(client, [{"flow-1", "lease-1", 1}], codec: Term)
  end

  test "batch acknowledgement responses remain opaque" do
    {:ok, client} = ReplyClient.start_link("OK")

    assert "OK" =
             Flow.create_many(client, ["flow-1"],
               type: "email",
               codec: Term,
               return_ok_on_success: true
             )
  end
end
