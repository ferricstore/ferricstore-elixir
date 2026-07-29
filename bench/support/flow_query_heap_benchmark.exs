defmodule FerricStore.FlowQueryHeapBenchmark do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.ResponseDecoderSpawnPolicy

  def print(name, payload, samples, decoder) do
    Enum.each([:default, :policy], fn minimum ->
      totals =
        Enum.reduce(
          1..samples,
          %{elapsed_us: 0, minor_gcs: 0, memory: 0, heap_size: 0, total_heap_size: 0},
          fn _, totals ->
            sample = sample(payload, minimum, decoder)
            Map.merge(totals, sample, fn _key, left, right -> left + right end)
          end
        )

      IO.puts(
        "#{name}_worker min_heap=#{minimum} samples=#{samples} " <>
          "avg_us=#{Float.round(totals.elapsed_us / samples, 2)} " <>
          "minor_gcs=#{Float.round(totals.minor_gcs / samples, 2)} " <>
          "memory_bytes=#{Float.round(totals.memory / samples, 1)} " <>
          "heap_words=#{Float.round(totals.heap_size / samples, 1)} " <>
          "total_heap_words=#{Float.round(totals.total_heap_size / samples, 1)}"
      )
    end)
  end

  defp sample(payload, minimum, decoder) do
    parent = self()

    options =
      case minimum do
        :default ->
          []

        :policy ->
          ResponseDecoderSpawnPolicy.options(Opcodes.flow_query(), byte_size(payload) + 2)
      end

    {pid, monitor} =
      :erlang.spawn_opt(
        fn ->
          started = System.monotonic_time(:microsecond)
          result = decoder.(payload)
          elapsed_us = System.monotonic_time(:microsecond) - started
          send(parent, {:decoded, self(), elapsed_us})

          receive do
            :finish ->
              _hash = :erlang.phash2(result)
              :ok
          end
        end,
        [:monitor | options]
      )

    elapsed_us = receive_decode(pid)
    info = process_info(pid)
    send(pid, :finish)
    await_exit(pid, monitor)

    %{
      elapsed_us: elapsed_us,
      minor_gcs: info.garbage_collection[:minor_gcs],
      memory: info.memory,
      heap_size: info.heap_size,
      total_heap_size: info.total_heap_size
    }
  end

  defp receive_decode(pid) do
    receive do
      {:decoded, ^pid, value} -> value
    after
      5_000 -> raise "flow-query heap sample timed out"
    end
  end

  defp process_info(pid) do
    pid
    |> Process.info([:memory, :heap_size, :total_heap_size, :garbage_collection])
    |> Map.new()
  end

  defp await_exit(pid, monitor) do
    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    after
      5_000 -> raise "flow-query heap sample did not terminate"
    end
  end
end
