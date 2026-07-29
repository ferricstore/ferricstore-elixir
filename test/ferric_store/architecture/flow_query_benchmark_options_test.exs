Code.require_file(
  "../../../bench/support/flow_query_benchmark_options.exs",
  __DIR__
)

defmodule FerricStore.Architecture.FlowQueryBenchmarkOptionsTest do
  use ExUnit.Case, async: true

  alias FerricStore.FlowQueryBenchmarkOptions

  test "rejects unknown and positional arguments so CI budgets cannot be skipped" do
    assert_raise ArgumentError, ~r/unknown benchmark arguments/, fn ->
      FlowQueryBenchmarkOptions.parse!(["--max-full-decode-reduction", "24000"])
    end

    assert_raise ArgumentError, ~r/unknown benchmark arguments/, fn ->
      FlowQueryBenchmarkOptions.parse!(["unexpected"])
    end
  end

  test "accepts the documented bounded workload and budgets" do
    opts =
      FlowQueryBenchmarkOptions.parse!([
        "--iterations",
        "500",
        "--records",
        "100",
        "--heap-samples",
        "0",
        "--max-full-decode-reductions",
        "24000"
      ])

    assert opts[:iterations] == 500
    assert opts[:records] == 100
    assert opts[:heap_samples] == 0
    assert opts[:max_full_decode_reductions] == 24_000
  end
end
