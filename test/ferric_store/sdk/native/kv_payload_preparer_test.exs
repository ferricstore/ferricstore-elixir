defmodule FerricStore.SDK.Native.KVPayloadPreparerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Protocol.{Opcodes, PreparedMap}
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{KVPayloadPreparer, Topology}
  alias FerricStore.Transport.{RequestEncoder, SessionPolicy}

  test "uses the protocol encoder as the exact KV body-size authority" do
    cases = [
      {:mget, ["one", "two"], %{"keys" => ["one", "two"]}},
      {
        :mset,
        [%{"key" => "key", "value" => %{"nested" => [1, true, nil]}}],
        %{"pairs" => [%{"key" => "key", "value" => %{"nested" => [1, true, nil]}}]}
      }
    ]

    Enum.each(cases, fn {operation, items, payload} ->
      encoded_bytes =
        payload |> Map.put("deadline_ms", 0) |> Protocol.encode_value() |> byte_size()

      context = RequestContext.new([], 5_000)

      assert {:ok, %{payload: %PreparedMap{} = prepared}} =
               operation
               |> group(items, payload, encoded_bytes)
               |> KVPayloadPreparer.prepare(operation, context)

      finalized = SessionPolicy.put_deadline(prepared, opcode(operation), 50)

      assert {:ok, decoded, ""} =
               finalized
               |> PreparedMap.to_iodata()
               |> IO.iodata_to_binary()
               |> Protocol.decode_value()

      assert Map.delete(decoded, "deadline_ms") == payload

      assert {:error, :request_too_large} =
               operation
               |> group(items, payload, encoded_bytes - 1)
               |> KVPayloadPreparer.prepare(operation, context)
    end)
  end

  test "prepared MSET values are not traversed again on the connection encoder path" do
    value = List.duplicate("nested-value", 50_000)
    items = [%{"key" => "key", "value" => value}]
    payload = %{"pairs" => items}

    max_request_bytes =
      payload |> Map.put("deadline_ms", 0) |> Protocol.encode_value() |> byte_size()

    context = RequestContext.new([], 5_000)

    assert {:ok, %{payload: %PreparedMap{} = prepared}} =
             :mset
             |> group(items, payload, max_request_bytes)
             |> KVPayloadPreparer.prepare(:mset, context)

    finalized = SessionPolicy.put_deadline(prepared, Opcodes.mset(), 50)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:ok, frame} =
             RequestEncoder.encode(Opcodes.mset(), 1, 7, finalized, max_request_bytes)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
    assert is_list(frame)
  end

  test "invalid MSET values fail during trusted preparation" do
    items = [%{"key" => "key", "value" => self()}]
    context = RequestContext.new([], 5_000)

    assert {:error, {:encode_failed, message}} =
             :mset
             |> group(items, %{"pairs" => items}, 1_000)
             |> KVPayloadPreparer.prepare(:mset, context)

    assert message =~ "cannot encode"
  end

  test "MSET preparation rejects a payload that diverges from its routed items" do
    items = [%{"key" => "routed-key", "value" => "routed-value"}]
    payload = %{"pairs" => [%{"key" => "stale-key", "value" => "stale-value"}]}
    context = RequestContext.new([], 5_000)

    assert {:error, {:invalid_prepared_payload, :mset}} =
             :mset
             |> group(items, payload, 1_000)
             |> KVPayloadPreparer.prepare(:mset, context)
  end

  test "MSET preparation rejects routed pair maps with unexpected fields" do
    pair = %{"key" => "key", "value" => "value", "unexpected" => true}
    context = RequestContext.new([], 5_000)

    assert {:error, {:invalid_prepared_payload, :mset}} =
             :mset
             |> group([pair], %{"pairs" => [pair]}, 1_000)
             |> KVPayloadPreparer.prepare(:mset, context)
  end

  test "MSET preparation rejects improper routed pairs without crashing" do
    context = RequestContext.new([], 5_000)
    pair = %{"key" => "key", "value" => "value"}
    improper_pairs = [pair | :invalid_tail]

    malformed_group =
      :mset
      |> group([pair], %{"pairs" => [pair]}, 1_000)
      |> Map.put(:items, improper_pairs)
      |> Map.put(:payload, %{"pairs" => improper_pairs})

    assert {:error, {:invalid_prepared_payload, :mset}} =
             KVPayloadPreparer.prepare(malformed_group, :mset, context)
  end

  test "DEL and MGET preparation reject payloads that diverge from routed items" do
    items = ["routed-key"]
    payload = %{"keys" => ["stale-key"]}
    context = RequestContext.new([], 5_000)

    for operation <- [:del, :mget] do
      assert {:error, {:invalid_prepared_payload, ^operation}} =
               operation
               |> group(items, payload, 1_000)
               |> KVPayloadPreparer.prepare(operation, context)
    end
  end

  defp group(operation, items, payload, max_request_bytes) do
    {:ok, topology} =
      Topology.build(topology_payload(),
        default_endpoint: %{
          host: "127.0.0.1",
          native_port: 6_388,
          max_request_bytes: max_request_bytes
        }
      )

    route_key = if operation == :mset, do: items |> hd() |> Map.fetch!("key"), else: hd(items)
    {:ok, route} = Topology.route_key(topology, route_key)

    %{
      route: route,
      items: items,
      indexes: Enum.to_list(0..(length(items) - 1)),
      payload: payload
    }
  end

  defp opcode(:mget), do: Opcodes.mget()
  defp opcode(:mset), do: Opcodes.mset()

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
          "node" => "preparer-test"
        }
      ]
    }
  end
end
