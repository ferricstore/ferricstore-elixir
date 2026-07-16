defmodule FerricStore.SDK.Native.BatchRetryTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.PreparedMap
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{BatchOperation, BatchRetry, KVBatchRestorer}

  test "defers compact input reconstruction to the sole retry preparation worker" do
    batch = prepared_batch(3)

    batch = %{
      batch
      | ready_groups: [%{indexes: [2], payload: prepared_payload(["third"])}],
        failures: [
          %{
            indexes: [0, 1],
            payload: prepared_payload(["first", "second"]),
            reason: :closed
          }
        ]
    }

    assert {:ok, retry} = BatchRetry.prepare(batch, :closed)
    assert %KVBatchRestorer{} = retry.item_restorer
    assert retry.preparation_mode == :restore_compact

    assert {:ok, ["first", "second", "third"]} =
             KVBatchRestorer.restore(retry.item_restorer, retry.items, retry.opts)

    assert retry.attempt == 1
    assert retry.original_reason == :closed
    assert retry.phase == :refreshing
    assert retry.ready_groups == []
    assert retry.failures == []

    assert %{items: nil} = BatchRetry.release_inputs(retry)
  end

  test "deferred retry restoration rejects incomplete or duplicate prepared groups" do
    batch = prepared_batch(2)

    duplicate = %{
      batch
      | failures: [
          %{indexes: [0], payload: prepared_payload(["first"]), reason: :closed},
          %{indexes: [0], payload: prepared_payload(["duplicate"]), reason: :closed}
        ]
    }

    assert {:ok, retry} = BatchRetry.prepare(duplicate, :closed)

    assert {:error, :invalid_prepared_groups} =
             KVBatchRestorer.restore(retry.item_restorer, retry.items, retry.opts)
  end

  defp prepared_batch(item_count) do
    opts = RequestContext.new([timeout: :infinity], 100)
    BatchOperation.new_prepared(:from, 3, :mget, item_count, & &1, &Map.new/1, opts)
  end

  defp prepared_payload(items) do
    {:ok, payload} = PreparedMap.prepare(%{"keys" => items}, 10_000, [{"deadline_ms", 0}])
    PreparedMap.put_metadata(payload, %{operation: :mget, items: items})
  end
end
