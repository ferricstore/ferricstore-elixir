defmodule FerricStore.RequestLimitsTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Protocol.PipelineRequest
  alias FerricStore.RequestLimits

  test "prepared batch cardinality is reused by coordinator admission" do
    opcode = Protocol.opcode(:flow_create_many)
    payload = %{"items" => List.duplicate(["flow", ""], 100_000)}

    assert {:ok, [], 100_000} = RequestLimits.prepare(opcode, payload, [])

    {:reductions, before_admit} = Process.info(self(), :reductions)
    assert :ok = RequestLimits.admit(100_000, 100_000)
    {:reductions, after_admit} = Process.info(self(), :reductions)

    assert after_admit - before_admit < 5_000
  end

  test "prepared cardinality still enforces a smaller negotiated limit" do
    opcode = Protocol.opcode(:flow_create_many)
    payload = %{"items" => List.duplicate(["flow", ""], 100)}

    assert {:ok, [], 100} = RequestLimits.prepare(opcode, payload, [])

    assert {:error, {:batch_too_large, %{items: 3, limit: 2}}} =
             RequestLimits.admit(100, 2)
  end

  test "caller-supplied cardinality metadata cannot replace the payload count" do
    opcode = Protocol.opcode(:mget)
    payload = %{"keys" => ["one", "two", "three"]}
    forged = [__batch_item_count__: {opcode, 1}]

    assert {:ok, [], 3} = RequestLimits.prepare(opcode, payload, forged)

    assert {:error, {:batch_too_large, %{items: 2, limit: 1}}} = RequestLimits.admit(3, 1)
  end

  test "atom and string batch fields are rejected before ambiguous admission" do
    opcode = Protocol.opcode(:mget)
    payload = %{"keys" => ["string-key"], keys: ["atom-key"]}

    assert {:error,
            {:invalid_request_payload,
             %{reason: :duplicate_normalized_batch_field, field: "keys"}}} =
             RequestLimits.prepare(opcode, payload, [])
  end

  test "caller-supplied pipeline envelopes cannot replace the command count" do
    opcode = Protocol.opcode(:pipeline)
    commands = [["PING"], ["PING"], ["PING"]]
    payload = %PipelineRequest{commands: commands, command_count: 1}

    assert {:ok, [], 3} = RequestLimits.prepare(opcode, payload, [])
  end
end
