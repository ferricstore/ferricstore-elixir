defmodule FerricStore.KVBenchmark do
  @moduledoc false

  def run(args) do
    opts = parse_args(args)
    url = Keyword.fetch!(opts, :url)
    command = Keyword.fetch!(opts, :command)
    requests = Keyword.fetch!(opts, :requests)
    batch = Keyword.fetch!(opts, :batch)
    clients = Keyword.fetch!(opts, :clients)
    min_throughput = Keyword.fetch!(opts, :min_throughput)
    value = :binary.copy("x", Keyword.fetch!(opts, :value_bytes))
    run_id = System.system_time(:nanosecond)

    client_pids =
      for _ <- 1..clients do
        FerricStore.connect!(
          url: url,
          client_name: "ferricstore-elixir-kv-benchmark",
          connect_timeout: 30_000,
          topology_refresh_timeout: 30_000
        )
      end

    if command == "get" do
      preload(client_pids, run_id, requests, batch, value)
    end

    started = System.monotonic_time(:millisecond)

    latencies =
      1..clients
      |> Enum.map(fn client_index ->
        client = Enum.at(client_pids, client_index - 1)
        Task.async(fn -> run_client(client, command, run_id, client_index, clients, requests, batch, value) end)
      end)
      |> Task.await_many(:infinity)
      |> List.flatten()

    elapsed_ms = max(System.monotonic_time(:millisecond) - started, 1)
    Enum.each(client_pids, &FerricStore.close/1)

    throughput = requests * 1000 / elapsed_ms
    enforce_minimum_throughput!(throughput, min_throughput)

    IO.puts(
      "command=#{command} requests=#{requests} clients=#{clients} batch=#{batch} elapsed_ms=#{elapsed_ms} throughput=#{Float.round(throughput, 2)}/s #{latency_summary(latencies)}"
    )
  end

  defp preload(clients, run_id, requests, batch, value) do
    client = hd(clients)

    1..requests
    |> Enum.chunk_every(batch)
    |> Enum.each(fn chunk ->
      commands =
        Enum.map(chunk, fn index ->
          %{opcode: FerricStore.Protocol.opcode(:set), body: %{"key" => key(run_id, index), "value" => value}}
        end)

      FerricStore.pipeline(client, commands, return: :compact, timeout: 30_000)
    end)
  end

  defp run_client(client, command, run_id, client_index, clients, requests, batch, value) do
    client_index..requests//clients
    |> Enum.chunk_every(batch)
    |> Enum.map(fn chunk ->
      commands = Enum.map(chunk, &command(command, run_id, &1, value))
      started = System.monotonic_time(:nanosecond)
      results = FerricStore.pipeline(client, commands, return: :compact, timeout: 30_000)
      elapsed = System.monotonic_time(:nanosecond) - started
      assert_ok!(results, command)
      elapsed
    end)
  end

  defp command("set", run_id, index, value), do: %{opcode: FerricStore.Protocol.opcode(:set), body: %{"key" => key(run_id, index), "value" => value}}
  defp command("get", run_id, index, _value), do: %{opcode: FerricStore.Protocol.opcode(:get), body: %{"key" => key(run_id, index)}}

  defp assert_ok!({:error, error}, command), do: raise("#{command} failed: #{inspect(error)}")

  defp assert_ok!(results, "set") do
    unless Enum.all?(results, &match?(["ok", "OK"], &1)) do
      raise "SET pipeline had failures: #{inspect(Enum.take(results, 3))}"
    end
  end

  defp assert_ok!(results, "get") do
    unless Enum.all?(results, &match?(["ok", value] when is_binary(value), &1)) do
      raise "GET pipeline had failures: #{inspect(Enum.take(results, 3))}"
    end
  end

  defp key(run_id, index), do: "elixir-kv:#{run_id}:#{index}"

  defp latency_summary([]), do: "batch_latency_samples=0"

  defp latency_summary(latencies) do
    sorted = Enum.sort(latencies)
    count = length(sorted)
    avg_ms = Enum.sum(sorted) / count / 1_000_000
    p50_ms = percentile(sorted, 50) / 1_000_000
    p95_ms = percentile(sorted, 95) / 1_000_000
    p99_ms = percentile(sorted, 99) / 1_000_000
    max_ms = List.last(sorted) / 1_000_000

    "batch_latency_samples=#{count} batch_latency_avg_ms=#{Float.round(avg_ms, 6)} batch_latency_p50_ms=#{Float.round(p50_ms, 6)} batch_latency_p95_ms=#{Float.round(p95_ms, 6)} batch_latency_p99_ms=#{Float.round(p99_ms, 6)} batch_latency_max_ms=#{Float.round(max_ms, 6)}"
  end

  defp percentile(sorted, percentile) do
    count = length(sorted)
    index = percentile |> Kernel./(100) |> Kernel.*(count) |> Float.ceil() |> trunc() |> Kernel.-(1)
    Enum.at(sorted, min(max(index, 0), count - 1))
  end

  defp enforce_minimum_throughput!(_throughput, nil), do: :ok

  defp enforce_minimum_throughput!(throughput, minimum) when throughput >= minimum, do: :ok

  defp enforce_minimum_throughput!(throughput, minimum) do
    raise "throughput regression: throughput=#{throughput}/s minimum=#{minimum}/s"
  end

  defp parse_args(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          url: :string,
          command: :string,
          requests: :integer,
          batch: :integer,
          clients: :integer,
          value_bytes: :integer,
          min_throughput: :float
        ]
      )

    opts
    |> Keyword.put_new(:url, System.get_env("FERRICSTORE_URL", "ferric://127.0.0.1:6388"))
    |> Keyword.put_new(:command, "get")
    |> Keyword.put_new(:requests, 100_000)
    |> Keyword.put_new(:batch, 100)
    |> Keyword.put_new(:clients, 8)
    |> Keyword.put_new(:value_bytes, 32)
    |> Keyword.put_new(:min_throughput, nil)
  end
end

FerricStore.KVBenchmark.run(System.argv())
