Code.require_file("support/flow_query_benchmark_payload.exs", __DIR__)
Code.require_file("support/flow_query_heap_benchmark.exs", __DIR__)
Code.require_file("support/flow_query_benchmark_options.exs", __DIR__)

defmodule FerricStore.FlowQueryClientBenchmark do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.{QueryResponse, RecordResponseDecoder}
  alias FerricStore.Protocol.FlowQueryResultDecoder
  alias FerricStore.{FlowQueryBenchmarkOptions, FlowQueryBenchmarkPayload, FlowQueryHeapBenchmark}

  def run(args) do
    opts = FlowQueryBenchmarkOptions.parse!(args)
    iterations = opts[:iterations]
    record_count = opts[:records]

    full_payload = FlowQueryBenchmarkPayload.page(record_count, :full)
    projected_payload = FlowQueryBenchmarkPayload.page(record_count, :projected)
    count_payload = FlowQueryBenchmarkPayload.count(record_count)

    full = decode_typed!(full_payload)
    projected = decode_typed!(projected_payload)
    count = decode_typed!(count_payload)

    verify!(full, projected, count, record_count)

    full_measurement = measure(iterations, fn -> decode_typed!(full_payload) end)
    projected_measurement = measure(iterations, fn -> decode_typed!(projected_payload) end)
    count_measurement = measure(iterations, fn -> decode_typed!(count_payload) end)

    budget = DeadlineBudget.new(:infinity)
    records = full.records

    raw_validation =
      measure(iterations, fn ->
        RecordResponseDecoder.decode_list_raw(records, :list, budget)
      end)

    enforce_reductions!("full_decode", full_measurement, opts[:max_full_decode_reductions])

    enforce_reductions!(
      "projected_decode",
      projected_measurement,
      opts[:max_projected_decode_reductions]
    )

    enforce_reductions!("count_decode", count_measurement, opts[:max_count_decode_reductions])

    enforce_reductions!(
      "raw_validation",
      raw_validation,
      opts[:max_raw_validation_reductions]
    )

    print_measurement("full_decode", full_payload, full, full_measurement)
    print_measurement("projected_decode", projected_payload, projected, projected_measurement)
    print_measurement("count_decode", count_payload, count, count_measurement)
    print_measurement("raw_validation", <<>>, records, raw_validation)

    if opts[:heap_samples] > 0 do
      decoder = &decode_raw!/1
      FlowQueryHeapBenchmark.print("full", full_payload, opts[:heap_samples], decoder)
      FlowQueryHeapBenchmark.print("projected", projected_payload, opts[:heap_samples], decoder)
      FlowQueryHeapBenchmark.print("count", count_payload, opts[:heap_samples], decoder)
    end
  end

  defp verify!(full, projected, count, record_count) do
    unless length(full.records) == record_count and
             length(projected.records) == record_count and
             count.count == record_count and
             Map.keys(hd(full.records)) |> length() ==
               FlowQueryBenchmarkPayload.field_count(:full) and
             Map.keys(hd(projected.records)) |> length() ==
               FlowQueryBenchmarkPayload.field_count(:projected) do
      raise "flow-query benchmark produced an invalid result"
    end
  end

  defp decode_typed!(payload) do
    with raw <- decode_raw!(payload),
         {:ok, typed} <- QueryResponse.result(raw) do
      typed
    else
      error -> raise "flow-query decode failed: #{inspect(error)}"
    end
  end

  defp decode_raw!(payload) do
    case FlowQueryResultDecoder.decode(payload) do
      {:ok, raw} -> raw
      error -> raise "flow-query compact decode failed: #{inspect(error)}"
    end
  end

  defp measure(iterations, function) do
    warmup = min(iterations, 25)
    Enum.each(1..warmup, fn _ -> function.() end)
    :erlang.garbage_collect(self())

    {:reductions, before_reductions} = Process.info(self(), :reductions)
    started = System.monotonic_time(:microsecond)
    result = Enum.reduce(1..iterations, nil, fn _, _previous -> function.() end)
    elapsed_us = System.monotonic_time(:microsecond) - started
    {:reductions, after_reductions} = Process.info(self(), :reductions)

    %{
      elapsed_us: elapsed_us,
      reductions: after_reductions - before_reductions,
      iterations: iterations,
      result: result
    }
  end

  defp print_measurement(name, payload, result, measurement) do
    iterations = measurement.iterations

    IO.puts(
      "#{name} iterations=#{iterations} wire_bytes=#{byte_size(payload)} " <>
        "shared_words=#{:erts_debug.size(result)} flat_words=#{:erts_debug.flat_size(result)} " <>
        "avg_us=#{Float.round(measurement.elapsed_us / iterations, 2)} " <>
        "reductions_per_iteration=#{Float.round(measurement.reductions / iterations, 1)}"
    )
  end

  defp enforce_reductions!(_name, _measurement, nil), do: :ok

  defp enforce_reductions!(name, measurement, maximum) do
    average = measurement.reductions / measurement.iterations

    if average > maximum,
      do: raise("#{name} reduction regression: reductions=#{average} budget=#{maximum}")
  end
end

FerricStore.FlowQueryClientBenchmark.run(System.argv())
