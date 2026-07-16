defmodule FerricStore.FlowRoutingTest do
  use ExUnit.Case, async: true

  alias FerricStore.FlowRouting
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.Topology

  test "auto-partitioned ids use the server's canonical routing tag" do
    id = "flow-id"
    expected = auto_route_key(id)

    assert {:ok, ^expected} = FlowRouting.resolve_payload(Opcodes.flow_get(), %{"id" => id}, [])
    refute Topology.slot_for_key(id) == Topology.slot_for_key(expected)
  end

  test "logical partitions use the server's SHA-256 routing tag" do
    partition = "tenant-route"
    expected = partition_route_key(partition)

    assert {:ok, ^expected} =
             FlowRouting.resolve_payload(
               Opcodes.flow_transition(),
               %{"id" => "flow-id", "partition_key" => partition},
               []
             )

    refute Topology.slot_for_key(partition) == Topology.slot_for_key(expected)
  end

  test "canonical auto-partition keys preserve their server bucket" do
    partition = "__flow_auto__:37"
    expected = "f:{fa:37}:route"

    assert {:ok, ^expected} =
             FlowRouting.resolve_payload(
               Opcodes.flow_complete(),
               %{"id" => "flow-id", "partition_key" => partition},
               []
             )
  end

  test "type-scoped and schedule commands stay on the control path" do
    assert :none =
             FlowRouting.resolve_payload(
               Opcodes.flow_policy_get(),
               %{"type" => "email"},
               []
             )

    assert :none =
             FlowRouting.resolve_payload(
               Opcodes.flow_schedule_get(),
               %{"id" => "schedule-id"},
               []
             )
  end

  test "value mget routes same-slot refs directly and keeps cross-slot reads on control" do
    first = "f:{flow-values}:v:first"
    second = "f:{flow-values}:v:second"

    assert {:ok, ^first} =
             FlowRouting.resolve_payload(
               Opcodes.flow_value_mget(),
               %{"refs" => [first, second]},
               []
             )

    other =
      Enum.find_value(1..1_024, fn candidate ->
        ref = "f:{other-#{candidate}}:v:third"
        if Topology.slot_for_key(ref) != Topology.slot_for_key(first), do: ref
      end)

    assert :none =
             FlowRouting.resolve_payload(
               Opcodes.flow_value_mget(),
               %{"refs" => [first, other]},
               []
             )
  end

  test "explicit physical route keys take precedence and remain validated" do
    assert {:ok, "f:{manual}:route"} =
             FlowRouting.resolve_payload(
               Opcodes.flow_get(),
               %{"id" => "flow-id"},
               route_key: "f:{manual}:route"
             )

    assert {:error, {:invalid_route_key, 123}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_get(),
               %{"id" => "flow-id"},
               route_key: 123
             )
  end

  test "logical partition and id routing enforce the server key-size contract" do
    oversized = :binary.copy("x", 65_536)
    error = {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}

    for {opcode, payload} <- [
          {Opcodes.flow_transition(), %{"id" => "flow-id", "partition_key" => oversized}},
          {Opcodes.flow_get(), %{"id" => oversized}},
          {Opcodes.flow_claim_due(), %{"partition_keys" => [oversized]}}
        ] do
      assert {:error, ^error} = FlowRouting.resolve_payload(opcode, payload, [])
    end
  end

  test "improper partition lists return a typed route error" do
    partitions = ["tenant-a" | "invalid-tail"]

    assert {:error, {:invalid_route_key, ^partitions}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_claim_due(),
               %{"partition_keys" => partitions},
               []
             )
  end

  test "partition list validation does not stop after routing becomes control scoped" do
    partitions = ["tenant-a", "tenant-b", 123]

    assert {:error, {:invalid_route_key, ^partitions}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_claim_due(),
               %{"partition_keys" => partitions},
               []
             )

    for invalid <- [[], [""]] do
      assert {:error, {:invalid_route_key, ^invalid}} =
               FlowRouting.resolve_payload(
                 Opcodes.flow_claim_due(),
                 %{"partition_keys" => invalid},
                 []
               )
    end
  end

  test "partition list validation still enforces key-size limits after routes diverge" do
    oversized = :binary.copy("x", 65_536)
    partitions = ["tenant-a", "tenant-b", oversized]

    assert {:error, {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_claim_due(),
               %{"partition_keys" => partitions},
               []
             )
  end

  test "oversized partition lists are rejected before hashing every partition" do
    partitions = List.duplicate("tenant-a", 100_001)
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_claim_due(),
               %{"partition_keys" => partitions},
               []
             )

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000_000
  end

  test "singular and plural partition selectors cannot silently override each other" do
    assert {:error, {:conflicting_route_fields, ["partition_key", "partition_keys"]}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_claim_due(),
               %{
                 "partition_key" => "tenant-a",
                 "partition_keys" => ["tenant-b"]
               },
               []
             )
  end

  test "atom and string spellings cannot select different routes" do
    assert {:error, {:duplicate_route_field, "key"}} =
             FlowRouting.resolve_payload(
               Opcodes.get(),
               %{"key" => "string-route", key: "atom-route"},
               []
             )

    assert {:error, {:duplicate_route_field, "partition_key"}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_transition(),
               %{
                 "id" => "flow-id",
                 "partition_key" => "string-partition",
                 partition_key: "atom-partition"
               },
               []
             )
  end

  test "duplicate route fields are invalid even when both values match" do
    assert {:error, {:duplicate_route_field, "id"}} =
             FlowRouting.resolve_payload(
               Opcodes.flow_get(),
               %{"id" => "same-flow", id: "same-flow"},
               []
             )
  end

  defp auto_route_key(id) do
    bucket = rem(:erlang.crc32(id), 256)
    "f:{fa:#{bucket}}:route"
  end

  defp partition_route_key(partition) do
    digest = partition |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)
    "f:{f:#{digest}}:route"
  end
end
