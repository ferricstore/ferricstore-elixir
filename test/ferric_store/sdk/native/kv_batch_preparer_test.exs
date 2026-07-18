defmodule FerricStore.SDK.Native.KVBatchPreparerTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{BatchRouter, KVBatchPreparer, KVBatchRestorer, Topology}

  test "expired preparation stops before routing a large batch" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: 0, call_timeout: 0], 5_000)
    assert {:error, :timeout} = KVBatchPreparer.prepare(topology, :mget, [], context)

    keys = Enum.map(1..100_000, &"expired-preparation-key-#{&1}")
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             KVBatchPreparer.prepare(topology, :mget, keys, context)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
  end

  test "batch routing uses countdown deadline checkpoints" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)
    keys = List.duplicate("routing-key", 100_000)
    router = fn key -> {:ok, key, key} end
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = BatchRouter.route(topology, keys, router, context)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:ok, [%{items: ^keys, indexes: indexes}]} = result
    assert length(indexes) == 100_000
    assert after_reductions - before_reductions < 2_350_000
  end

  test "map batch routing preserves entries and propagates item errors" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)
    items = %{"alpha" => 1, "beta" => 2, "gamma" => 3}
    router = fn {key, value} -> {:ok, key, {key, value}} end

    assert {:ok, [%{items: routed_items, indexes: [0, 1, 2]}]} =
             BatchRouter.route(topology, items, router, context)

    assert Map.new(routed_items) == items

    assert {:error, :invalid_item} =
             BatchRouter.route(topology, items, fn _item -> {:error, :invalid_item} end, context)
  end

  test "prepared-item recovery rejects improper group lists without raising" do
    groups = [%{indexes: [0], items: ["key"]} | :invalid_tail]
    context = RequestContext.new([timeout: :infinity], 5_000)
    restorer = KVBatchRestorer.new(1, nil)

    assert {:error, :invalid_prepared_groups} =
             KVBatchRestorer.restore(restorer, groups, context)
  end

  test "expired prepared-item recovery stops before traversing a large batch" do
    item_count = 100_000

    groups = [
      %{indexes: Enum.to_list(0..(item_count - 1)), items: List.duplicate("key", item_count)}
    ]

    context = RequestContext.new([timeout: 0], 5_000)
    restorer = KVBatchRestorer.new(item_count, nil)

    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :timeout} = KVBatchRestorer.restore(restorer, groups, context)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
  end

  test "batch routing rejects improper item lists without raising" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)
    items = ["key" | :invalid_tail]

    assert {:error, {:invalid_batch_items, :improper_list}} =
             BatchRouter.route(topology, items, fn key -> {:ok, key, key} end, context)

    expired = RequestContext.new([timeout: 0], 5_000)
    assert {:error, :timeout} = BatchRouter.route(topology, items, fn key -> key end, expired)
  end

  test "batch routing contains item-router failures" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)

    assert {:error, {:route_item_failed, "item routing failed"}} =
             BatchRouter.route(
               topology,
               ["key"],
               fn _key ->
                 raise FerricStore.Test.ExplodingError
               end,
               context
             )

    assert {:error, {:route_item_failed, {:throw, :router_failed}}} =
             BatchRouter.route(topology, ["key"], fn _key -> throw(:router_failed) end, context)
  end

  test "compact KV preparation rejects non-binary keys without raising" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)

    for operation <- [:mget, :del], invalid_key <- [:atom, 42, nil, %{}] do
      assert {:error, {:invalid_route_key, ^invalid_key}} =
               KVBatchPreparer.prepare(topology, operation, [invalid_key], context)
    end
  end

  test "compact KV preparation rejects keys beyond the server contract" do
    {:ok, topology} = Topology.build(topology_payload())
    context = RequestContext.new([timeout: :infinity], 5_000)
    oversized = :binary.copy("k", 65_536)

    for operation <- [:mget, :del] do
      assert {:error, {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}} =
               KVBatchPreparer.prepare(topology, operation, [oversized], context)
    end

    assert {:error, {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}} =
             KVBatchPreparer.prepare(topology, :mset, [{oversized, "value"}], context)
  end

  test "MSET groups by hash slot and rejects cross-slot writes" do
    {:ok, topology} = Topology.build(topology_payload())
    [first, second] = keys_in_distinct_slots()
    pairs = [{first, "first"}, {second, "second"}]

    atomic =
      RequestContext.new(
        [timeout: :infinity, require_same_slot: :mset],
        5_000
      )

    assert {:error, {:cross_slot_keys, :mset}} =
             KVBatchPreparer.prepare(topology, :mset, pairs, atomic)

    per_slot = RequestContext.new([timeout: :infinity, atomicity: :per_slot], 5_000)

    assert {:ok, groups} = KVBatchPreparer.prepare(topology, :mset, pairs, per_slot)
    assert Enum.map(groups, & &1.indexes) == [[0], [1]]
  end

  defp topology_payload do
    %{
      "route_epoch" => 1,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1_023,
          "shard" => 0,
          "lane_id" => 1,
          "node" => "preparer-test",
          "host" => "127.0.0.1",
          "native_port" => 6_388
        }
      ]
    }
  end

  defp keys_in_distinct_slots do
    first = "mset-slot-0"
    first_slot = Topology.slot_for_key(first)

    second =
      Enum.find_value(1..10_000, fn suffix ->
        candidate = "mset-slot-#{suffix}"
        if Topology.slot_for_key(candidate) != first_slot, do: candidate
      end)

    [first, second]
  end
end
