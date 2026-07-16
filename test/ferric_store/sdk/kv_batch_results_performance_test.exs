defmodule FerricStore.SDK.KVBatchResultsPerformanceTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.KV.{BatchResults, Response}

  test "dense-prefix mget size mismatches are validated in one linear pass" do
    indexes = Enum.to_list(0..99_999)
    values = List.duplicate("value", 99_999)

    {:reductions, before_count} = Process.info(self(), :reductions)
    result = BatchResults.mget([%{indexes: indexes, value: values}], 100_000)
    {:reductions, after_count} = Process.info(self(), :reductions)

    assert {:error, {:mismatched_mget_response, %{expected: 100_000, actual: 99_999}}} = result

    assert after_count - before_count < 180_000
  end

  test "exact HMGET replies validate type and cardinality in one pass" do
    values = List.duplicate("value", 100_000)

    {:reductions, before_count} = Process.info(self(), :reductions)
    result = Response.exact_list({:ok, values}, :hmget, 100_000)
    {:reductions, after_count} = Process.info(self(), :reductions)

    assert {:ok, ^values} = result
    assert after_count - before_count < 160_000
  end

  test "oversized HMGET replies stop at the first excess item" do
    values = List.duplicate("value", 100_000)

    {:reductions, before_count} = Process.info(self(), :reductions)
    result = Response.exact_list({:ok, values}, :hmget, 0)
    {:reductions, after_count} = Process.info(self(), :reductions)

    assert {:error, {:invalid_kv_response, %{operation: :hmget, reason: :unexpected_cardinality}}} =
             result

    assert after_count - before_count < 10_000
  end
end
