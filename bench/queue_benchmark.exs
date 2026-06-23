defmodule FerricStore.QueueBenchmark do
  @moduledoc false

  def run(args) do
    opts = parse_args(args)
    url = Keyword.fetch!(opts, :url)
    flows = Keyword.fetch!(opts, :flows)
    batch = Keyword.fetch!(opts, :batch)

    client = FerricStore.connect!(url: url, client_name: "ferricstore-elixir-benchmark", timeout: 30_000)
    run_id = System.system_time(:nanosecond)
    type = "elixir-benchmark-#{run_id}"
    queue = FerricStore.Queue.new(client, type, worker: "bench-worker")

    started = System.monotonic_time(:millisecond)

    1..flows
    |> Enum.chunk_every(batch)
    |> Enum.each(fn chunk ->
      commands =
        Enum.map(chunk, fn index ->
          id = "elixir-bench-#{run_id}-#{index}"

          %{
            opcode: FerricStore.Protocol.opcode(:flow_create),
            body: FerricStore.Flow.create_payload(id, type: queue.type, state: queue.state, payload: "payload")
          }
        end)

      client
      |> FerricStore.pipeline(commands, timeout: 30_000)
      |> assert_pipeline_ok!("create")
    end)

    claimed = drain(queue, flows, batch, 0, 0)
    elapsed_ms = max(System.monotonic_time(:millisecond) - started, 1)
    FerricStore.close(client)

    throughput = claimed * 1000 / elapsed_ms
    IO.puts("flows=#{claimed} elapsed_ms=#{elapsed_ms} throughput=#{Float.round(throughput, 2)}/s")
  end

  defp drain(_queue, target, _batch, done, _empty_claims) when done >= target, do: done

  defp drain(_queue, _target, _batch, done, empty_claims) when empty_claims > 1_000 do
    raise "benchmark made no claim progress after #{empty_claims} empty claims; completed=#{done}"
  end

  defp drain(queue, target, batch, done, empty_claims) do
    jobs = FerricStore.Queue.claim(queue, limit: batch, timeout: 30_000)

    case jobs do
      [] ->
        Process.sleep(1)
        drain(queue, target, batch, done, empty_claims + 1)

      jobs ->
        commands =
          Enum.map(jobs, fn job ->
            %{
              opcode: FerricStore.Protocol.opcode(:flow_complete),
              body:
                FerricStore.Flow.complete_payload(Map.fetch!(job, "id"),
                  lease_token: Map.fetch!(job, "lease_token"),
                  fencing_token: Map.fetch!(job, "fencing_token"),
                  result: "ok"
                )
            }
          end)

        queue.client
        |> FerricStore.pipeline(commands, timeout: 30_000)
        |> assert_pipeline_ok!("complete")

        drain(queue, target, batch, done + length(jobs), 0)
    end
  end

  defp assert_pipeline_ok!({:error, error}, phase), do: raise("#{phase} pipeline failed: #{inspect(error)}")

  defp assert_pipeline_ok!(results, phase) when is_list(results) do
    bad = Enum.reject(results, &(Map.get(&1, "status") in ["ok", :ok]))

    if bad != [] do
      raise "#{phase} pipeline returned failures: #{inspect(Enum.take(bad, 3))}"
    end

    results
  end

  defp parse_args(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [url: :string, flows: :integer, batch: :integer]
      )

    opts
    |> Keyword.put_new(:url, System.get_env("FERRICSTORE_URL", "ferric://127.0.0.1:6388"))
    |> Keyword.put_new(:flows, 10_000)
    |> Keyword.put_new(:batch, 100)
  end
end

FerricStore.QueueBenchmark.run(System.argv())
