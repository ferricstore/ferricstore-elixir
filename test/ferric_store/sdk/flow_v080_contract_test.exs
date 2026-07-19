defmodule FerricStore.SDK.FlowV080ContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.PolicySnapshot
  alias FerricStore.Protocol
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    alias FerricStore.Protocol.Opcodes

    def start_link(owner),
      do: GenServer.start_link(__MODULE__, owner) |> ClientRuntime.wrap()

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, owner) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, owner)
    end

    def handle_call({:request, opcode, payload, _context}, _from, owner) do
      send(owner, {:flow_request, opcode, nil, payload})
      {:reply, {:ok, reply(opcode, payload)}, owner}
    end

    def handle_call({:command, opcode, route, payload, _context}, _from, owner) do
      send(owner, {:flow_request, opcode, route, payload})
      {:reply, {:ok, reply(opcode, payload)}, owner}
    end

    defp reply(unquote(Opcodes.flow_policy_set()), payload),
      do: Map.merge(payload, %{"generation" => 1, "states" => %{}})

    defp reply(_opcode, payload), do: payload
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    {:ok, client: client}
  end

  test "orchestration commands carry finite and infinite max_active_ms values", %{client: client} do
    calls = [
      {:start_and_claim, Opcodes.flow_start_and_claim(),
       %{id: "root", type: "email", initial_state: "queued", worker: "w1", max_active_ms: 30_000}},
      {:spawn_children, Opcodes.flow_spawn_children(),
       %{
         id: "root",
         partition_key: "tenant-a",
         group_id: "children",
         fencing_token: 1,
         max_active_ms: :infinity,
         children: [%{id: "child", max_active_ms: 10_000}]
       }}
    ]

    Enum.each(calls, fn {function, opcode, payload} ->
      assert {:ok, normalized} = apply(Flow, function, [client, payload])
      assert normalized["max_active_ms"] == payload.max_active_ms
      assert_receive {:flow_request, ^opcode, _route, ^normalized}
    end)
  end

  test "create-many preserves type and per-item max_active_ms values", %{client: client} do
    payload = %{
      type: "email",
      max_active_ms: :infinity,
      items: [%{id: "first", max_active_ms: 5_000}, %{id: "second", max_active_ms: :infinity}]
    }

    assert {:ok, normalized} = Flow.create_many(client, payload)
    assert normalized["max_active_ms"] == :infinity

    assert {:ok, wire_payload, ""} =
             normalized |> Protocol.encode_value() |> Protocol.decode_value()

    assert Enum.map(wire_payload["items"], & &1["max_active_ms"]) == [5_000, "infinity"]
    assert_receive {:flow_request, opcode, nil, ^normalized}
    assert opcode == Opcodes.flow_create_many()
  end

  test "type policy carries max_active_ms infinity on the control path", %{client: client} do
    assert {:ok, %PolicySnapshot{type: "email", generation: 1, max_active_ms: :infinity}} =
             Flow.policy_set(client, %{type: "email", max_active_ms: :infinity})

    assert_receive {:flow_request, opcode, nil,
                    %{"type" => "email", "max_active_ms" => :infinity}}

    assert opcode == Opcodes.flow_policy_set()
  end
end
