defmodule FerricStore.SDK.ManagementInputContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Management
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
      send(owner, {:management_payload, payload})
      {:reply, {:ok, payload}, owner}
    end
  end

  test "ACL rules reject empty and boolean-like sentinel values before transport" do
    {:ok, client} = CaptureClient.start_link(self())

    for rule <- [nil, true, false, "", String.to_atom("")] do
      assert {:error,
              {:invalid_management_input,
               %{
                 operation: :set_user,
                 field: :rules,
                 reason: :invalid_rule,
                 index: 0,
                 value: ^rule
               }}} = Management.set_user(client, "worker", [rule])
    end

    refute_received {:management_payload, _payload}
  end

  test "valid ACL atom rules remain supported" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:ok, %{"args" => ["SETUSER", "worker", "on"]}} =
             Management.set_user(client, "worker", [:on])
  end
end
