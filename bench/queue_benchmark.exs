defmodule FerricStore.QueueBenchmark do
  @moduledoc false

  @auto_partition_prefix "__flow_auto__:"
  @auto_partition_buckets 256
  @server_slot_count 1024

  def run(args) do
    opts = parse_args(args)
    url = Keyword.fetch!(opts, :url)
    flows = Keyword.fetch!(opts, :flows)
    producers = Keyword.fetch!(opts, :producers)
    workers = Keyword.fetch!(opts, :workers)
    create_batch = Keyword.fetch!(opts, :create_batch)
    create_mode = Keyword.fetch!(opts, :create_mode)
    create_inflight_batches = Keyword.fetch!(opts, :create_inflight_batches)
    create_auto_buckets = Keyword.fetch!(opts, :create_auto_buckets)
    claim_batch = Keyword.fetch!(opts, :claim_batch)
    claim_partition_batch = Keyword.fetch!(opts, :claim_partition_batch)
    claim_drain_batches = Keyword.fetch!(opts, :claim_drain_batches)
    complete_async_depth = Keyword.fetch!(opts, :complete_async_depth)
    worker_connections = max(Keyword.fetch!(opts, :worker_connections), 1)
    server_shards = Keyword.fetch!(opts, :server_shards)
    timeout = Keyword.fetch!(opts, :timeout)

    run_id = System.system_time(:nanosecond)
    type = "elixir-benchmark-#{run_id}"
    counters = :atomics.new(2, [])

    producer_clients =
      for index <- 1..producers do
        connect!(url, "ferricstore-elixir-producer-#{index}", timeout)
      end

    worker_client_pool =
      for index <- 1..worker_connections do
        connect!(url, "ferricstore-elixir-worker-connection-#{index}", timeout)
      end

    worker_clients =
      for index <- 1..workers do
        Enum.at(worker_client_pool, rem(index - 1, worker_connections))
      end

    started = System.monotonic_time(:millisecond)

    worker_tasks =
      worker_clients
      |> Enum.with_index(1)
      |> Enum.map(fn {client, index} ->
        Task.async(fn ->
          drain_worker(
            client,
            type,
            index,
            workers,
            flows,
            claim_batch,
            counters,
            timeout,
            claim_partition_batch,
            claim_drain_batches,
            complete_async_depth,
            owned_auto_partitions(index - 1, workers, server_shards)
          )
        end)
      end)

    producer_clients
    |> Enum.with_index(1)
    |> Enum.map(fn {client, index} ->
      Task.async(fn ->
        create_flows(
          client,
          type,
          run_id,
          index,
          producers,
          flows,
          create_batch,
          create_mode,
          create_inflight_batches,
          create_auto_buckets,
          timeout
        )
      end)
    end)
    |> Task.await_many(:infinity)

    create_elapsed_ms = max(System.monotonic_time(:millisecond) - started, 1)

    Task.await_many(worker_tasks, :infinity)

    claimed = :atomics.get(counters, 2)
    elapsed_ms = max(System.monotonic_time(:millisecond) - started, 1)
    Enum.each(producer_clients ++ worker_client_pool, &FerricStore.close/1)

    throughput = claimed * 1000 / elapsed_ms
    create_throughput = flows * 1000 / create_elapsed_ms

    IO.puts(
      "flows=#{claimed} producers=#{producers} workers=#{workers} worker_connections=#{worker_connections} create_batch=#{create_batch} create_mode=#{create_mode} create_inflight_batches=#{create_inflight_batches} create_auto_buckets=#{create_auto_buckets} claim_batch=#{claim_batch} claim_partition_batch=#{claim_partition_batch} claim_drain_batches=#{claim_drain_batches} complete_async_depth=#{complete_async_depth} create_elapsed_ms=#{create_elapsed_ms} create_throughput=#{Float.round(create_throughput, 2)}/s elapsed_ms=#{elapsed_ms} throughput=#{Float.round(throughput, 2)}/s"
    )
  end

  defp connect!(url, name, timeout) do
    FerricStore.connect!(url: url, client_name: name, timeout: timeout)
  end

  defp create_flows(
         client,
         type,
         run_id,
         producer_index,
         producers,
         flows,
         batch,
         mode,
         inflight,
       create_auto_buckets,
       timeout
       ) do
    if create_auto_buckets and mode == "many" do
      create_flows_by_auto_partition(
        client,
        type,
        run_id,
        producer_index,
        producers,
        flows,
        batch,
        inflight,
        timeout
      )
    else
      producer_index..flows//producers
      |> Enum.chunk_every(batch)
      |> Enum.reduce([], fn chunk, pending ->
        items = Enum.map(chunk, &flow_id(run_id, &1))

        pending =
          submit_create_with_backpressure(
            pending,
            client,
            type,
            nil,
            items,
            mode,
            create_auto_buckets,
            inflight,
            timeout
          )

        pending
      end)
      |> await_all_create()
    end
  end

  defp create_flows_by_auto_partition(
         client,
         type,
         run_id,
         producer_index,
         producers,
         flows,
         batch,
         inflight,
         timeout
       ) do
    {buffers, pending} =
      1..flows
      |> Stream.map(&flow_id(run_id, &1))
      |> Stream.filter(&(rem(auto_partition_index_for_id(&1), producers) == producer_index - 1))
      |> Enum.reduce({%{}, []}, fn id, {buffers, pending} ->
        partition_key = auto_partition_key_for_id(id)
        {count, ids} = Map.get(buffers, partition_key, {0, []})
        count = count + 1
        ids = [id | ids]

        if count >= batch do
          pending =
            submit_create_with_backpressure(
              pending,
              client,
              type,
              partition_key,
              Enum.reverse(ids),
              "many",
              false,
              inflight,
              timeout
            )

          {Map.put(buffers, partition_key, {0, []}), pending}
        else
          {Map.put(buffers, partition_key, {count, ids}), pending}
        end
      end)

    buffers
    |> Enum.reduce(pending, fn
      {_partition_key, {0, []}}, pending ->
        pending

      {partition_key, {_count, ids}}, pending ->
        submit_create_with_backpressure(
          pending,
          client,
          type,
          partition_key,
          Enum.reverse(ids),
          "many",
          false,
          inflight,
          timeout
        )
    end)
    |> await_all_create()
  end

  defp submit_create_with_backpressure(
         pending,
         client,
         type,
         partition_key,
         items,
         mode,
         create_auto_buckets,
         inflight,
         timeout
       ) do
    pending = reap_create_tasks(pending)
    pending = if length(pending) >= inflight, do: await_one_create(pending), else: pending
    [submit_create(client, type, partition_key, items, mode, create_auto_buckets, timeout) | pending]
  end

  defp submit_create(client, type, partition_key, items, "many", _create_auto_buckets, timeout) do
    {:ok, payload} =
      FerricStore.Protocol.compact_flow_create_many_ids_payload(
        type,
        "queued",
        partition_key,
        items,
        independent: true,
        return_ok_on_success: true
      )

    ref =
      FerricStore.async_native(
        client,
        FerricStore.Protocol.opcode(:flow_create_many),
        FerricStore.Protocol.custom_payload(payload),
        timeout: timeout
      )

    {ref, :create_many, length(items), timeout}
  end

  defp submit_create(client, type, _partition_key, items, "pipeline", create_auto_buckets, timeout) do
    commands =
      Enum.map(items, fn id ->
        opts = [type: type, state: "queued"]
        opts = if create_auto_buckets, do: Keyword.put(opts, :partition_key, auto_partition_key_for_id(id)), else: opts

        %{
          opcode: FerricStore.Protocol.opcode(:flow_create),
          body: FerricStore.Flow.create_payload(id, opts)
        }
      end)

    ref = FerricStore.async_pipeline(client, commands, return: :compact, timeout: timeout)
    {ref, :create_pipeline, length(items), timeout}
  end

  defp drain_worker(
         client,
         type,
         worker_index,
         worker_count,
         target,
         batch,
         counters,
         timeout,
         claim_partition_batch,
         claim_drain_batches,
         complete_async_depth,
         owned_partitions
       ) do
    drain_worker(
      client,
      type,
      worker_index,
      worker_count,
      target,
      batch,
      counters,
      timeout,
      claim_partition_batch,
      claim_drain_batches,
      owned_partitions,
      0,
      0,
      [],
      0,
      complete_async_depth
    )
  end

  defp drain_worker(
         _client,
         _type,
         worker_index,
         _worker_count,
         _target,
         _batch,
         counters,
         _timeout,
         _claim_partition_batch,
         _claim_drain_batches,
         _owned_partitions,
         _claim_round,
         empty_claims,
         _pending,
         _complete_cursor,
         _complete_async_depth
       )
       when empty_claims > 10_000 do
    raise "worker #{worker_index} made no claim progress after #{empty_claims} empty claims; completed=#{:atomics.get(counters, 2)}"
  end

  defp drain_worker(
         client,
         type,
         worker_index,
         worker_count,
         target,
         batch,
         counters,
         timeout,
         claim_partition_batch,
         claim_drain_batches,
         owned_partitions,
         claim_round,
         empty_claims,
         pending,
         complete_cursor,
         complete_async_depth
       ) do
    pending = reap_completed(pending, counters)

    cond do
      :atomics.get(counters, 2) >= target and pending == [] ->
        :ok

      :atomics.get(counters, 1) >= target ->
        pending = await_all(pending, counters)

        drain_worker(
          client,
          type,
          worker_index,
          worker_count,
          target,
          batch,
          counters,
          timeout,
          claim_partition_batch,
          claim_drain_batches,
          owned_partitions,
          claim_round,
          empty_claims,
          pending,
          complete_cursor,
          complete_async_depth
        )

      length(pending) >= complete_async_depth ->
        pending = await_one(pending, counters)

        drain_worker(
          client,
          type,
          worker_index,
          worker_count,
          target,
          batch,
          counters,
          timeout,
          claim_partition_batch,
          claim_drain_batches,
          owned_partitions,
          claim_round,
          empty_claims,
          pending,
          complete_cursor,
          complete_async_depth
        )

      true ->
      {claim_partition_keys, next_claim_round} =
        claim_partition_keys(owned_partitions, claim_round, claim_partition_batch)

      jobs =
        FerricStore.Flow.claim_due(client, type,
            state: "queued",
            worker: "bench-worker-#{worker_index}",
            lease_ms: 30_000,
            limit: min(min(batch * claim_drain_batches, 1_000), target - :atomics.get(counters, 1)),
            include_attributes: false,
            partition_keys: claim_partition_keys
          )

        case jobs do
          {:error, error} ->
            raise "claim failed: #{inspect(error)}"

          [] ->
            Process.sleep(1)

          drain_worker(
              client,
              type,
              worker_index,
            worker_count,
            target,
            batch,
            counters,
            timeout,
            claim_partition_batch,
            claim_drain_batches,
            owned_partitions,
            next_claim_round,
              empty_claims + 1,
              pending,
              complete_cursor,
              complete_async_depth
            )

          jobs ->
            :atomics.add(counters, 1, length(jobs))
            {pending, complete_cursor} =
              submit_or_complete_sync(
                client,
                complete_async_depth,
                complete_cursor,
                jobs,
                pending,
                counters
              )

            drain_worker(
              client,
              type,
              worker_index,
            worker_count,
            target,
            batch,
            counters,
            timeout,
            claim_partition_batch,
            claim_drain_batches,
            owned_partitions,
            next_claim_round,
              0,
              pending,
              complete_cursor,
              complete_async_depth
            )
        end
    end
  end

  defp submit_or_complete_sync(client, 1, cursor, jobs, pending, counters) do
    FerricStore.Flow.complete_many(client, jobs, return_ok_on_success: true)
    |> assert_many_ok!("complete", length(jobs))

    :atomics.add(counters, 2, length(jobs))
    {pending, cursor}
  end

  defp submit_or_complete_sync(client, _depth, cursor, jobs, pending, _counters) do
    {task, cursor} = submit_completion(client, cursor, jobs)
    {[task | pending], cursor}
  end

  defp submit_completion(client, cursor, jobs) do
    payload =
      FerricStore.Flow.complete_many_payload(jobs, return_ok_on_success: true)
      |> compact_or_typed(&FerricStore.Protocol.compact_flow_complete_many_payload/1)

    ref =
      FerricStore.async_native(
        client,
        FerricStore.Protocol.opcode(:flow_complete_many),
        payload
      )

    {{ref, :complete_many, length(jobs), 30_000}, cursor + 1}
  end

  defp reap_create_tasks(requests) do
    requests
    |> Enum.reduce([], fn
      request, acc ->
        case yield_request(request) do
          :pending -> [request | acc]
          count when is_integer(count) -> acc
        end
    end)
    |> Enum.reverse()
  end

  defp await_one_create([]), do: []

  defp await_one_create([request | rest]) do
    await_request!(request)
    rest
  end

  defp await_all_create(requests) do
    Enum.each(requests, &await_request!/1)
  end

  defp reap_completed(requests, counters) do
    requests
    |> Enum.reduce([], fn
      request, acc ->
        case yield_request(request) do
          :pending ->
            [request | acc]

          count when is_integer(count) ->
            :atomics.add(counters, 2, count)
            acc
        end
    end)
    |> Enum.reverse()
  end

  defp await_one([], _counters), do: []

  defp await_one([request | rest], counters) do
    count = await_request!(request)
    :atomics.add(counters, 2, count)
    rest
  end

  defp await_all(requests, counters) do
    Enum.each(requests, fn request ->
      count = await_request!(request)
      :atomics.add(counters, 2, count)
    end)

    []
  end

  defp yield_request({ref, phase, count, _timeout}) do
    case FerricStore.yield(ref, 0) do
      nil ->
        :pending

      {:ok, response} ->
        validate_response!(response, phase, count)
    end
  end

  defp await_request!({ref, phase, count, timeout}) do
    ref
    |> FerricStore.await(timeout)
    |> validate_response!(phase, count)
  end

  defp validate_response!(response, :create_pipeline, count) do
    assert_pipeline_ok!(response, "create")
    count
  end

  defp validate_response!(response, phase, count) when phase in [:create_many, :complete_many] do
    assert_many_ok!(response, Atom.to_string(phase), count)
    count
  end

  defp compact_or_typed(payload, compact_fun) do
    case compact_fun.(payload) do
      {:ok, compact_payload} -> FerricStore.Protocol.custom_payload(compact_payload)
      :error -> payload
    end
  end

  defp owned_auto_partitions(worker_index, workers, server_shards) do
    0..(@auto_partition_buckets - 1)
    |> Enum.filter(&(auto_partition_owner(&1, workers, server_shards) == worker_index))
  end

  defp auto_partition_owner(index, workers, server_shards) do
    workers = max(workers, 1)
    server_shards = max(server_shards, 1)
    shard = auto_partition_server_shard_for_index(index, server_shards)

    if workers <= server_shards do
      rem(shard, workers)
    else
      shard_workers = Enum.filter(0..(workers - 1), &(rem(&1, server_shards) == shard))

      case shard_workers do
        [] -> rem(shard, workers)
        workers_for_shard -> Enum.at(workers_for_shard, rem(index, length(workers_for_shard)))
      end
    end
  end

  defp auto_partition_server_shard_for_index(index, server_shards) do
    tag = "fa:#{rem(index, @auto_partition_buckets)}"
    slot = Bitwise.band(:erlang.crc32(tag), @server_slot_count - 1)
    server_shard_for_slot(slot, server_shards)
  end

  defp server_shard_for_slot(slot, server_shards) do
    server_shards = max(server_shards, 1)
    slots_per_shard = div(@server_slot_count, server_shards)
    remainder = rem(@server_slot_count, server_shards)
    wide_slots = (slots_per_shard + 1) * remainder
    slot = rem(slot, @server_slot_count)

    if slot < wide_slots do
      div(slot, slots_per_shard + 1)
    else
      remainder + div(slot - wide_slots, slots_per_shard)
    end
  end

  defp claim_partition_keys([], claim_round, _count), do: {nil, claim_round + 1}

  defp claim_partition_keys(owned_partitions, claim_round, count) do
    count = min(max(count, 1), length(owned_partitions))
    start = rem(claim_round, length(owned_partitions))

    keys =
      for offset <- 0..(count - 1) do
        owned_partitions
        |> Enum.at(rem(start + offset, length(owned_partitions)))
        |> auto_partition_key_for_index()
      end

    {keys, claim_round + count}
  end

  defp auto_partition_key_for_index(index),
    do: "#{@auto_partition_prefix}#{rem(index, @auto_partition_buckets)}"

  defp auto_partition_key_for_id(id),
    do: id |> :erlang.crc32() |> rem(@auto_partition_buckets) |> auto_partition_key_for_index()

  defp auto_partition_index_for_id(id), do: id |> :erlang.crc32() |> rem(@auto_partition_buckets)

  defp flow_id(run_id, index), do: "elixir-bench-#{run_id}-#{index}"

  defp assert_many_ok!({:error, error}, phase, _count), do: raise("#{phase} many failed: #{inspect(error)}")
  defp assert_many_ok!("OK", _phase, _count), do: "OK"

  defp assert_many_ok!(results, phase, count) when is_list(results) do
    if length(results) != count or not Enum.all?(results, &(&1 in ["OK", "ok", :ok])) do
      raise "#{phase} many returned failures: #{inspect(Enum.take(results, 3))}"
    end

    results
  end

  defp assert_pipeline_ok!({:error, error}, phase), do: raise("#{phase} pipeline failed: #{inspect(error)}")

  defp assert_pipeline_ok!(results, phase) when is_list(results) do
    bad = Enum.reject(results, &pipeline_ok?/1)

    if bad != [] do
      raise "#{phase} pipeline returned failures: #{inspect(Enum.take(bad, 3))}"
    end

    results
  end

  defp pipeline_ok?(["ok", "OK"]), do: true
  defp pipeline_ok?(%{"status" => status}) when status in ["ok", :ok], do: true
  defp pipeline_ok?(_result), do: false

  defp parse_args(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          url: :string,
          flows: :integer,
          batch: :integer,
          create_batch: :integer,
          create_mode: :string,
          create_inflight_batches: :integer,
          create_auto_buckets: :boolean,
          claim_batch: :integer,
          claim_partition_batch: :integer,
          claim_drain_batches: :integer,
          complete_async_depth: :integer,
          worker_connections: :integer,
          producers: :integer,
          workers: :integer,
          server_shards: :integer,
          timeout: :integer
        ]
      )

    batch = Keyword.get(opts, :batch, 500)

    opts
    |> Keyword.put_new(:url, System.get_env("FERRICSTORE_URL", "ferric://127.0.0.1:6388"))
    |> Keyword.put_new(:flows, 10_000)
    |> Keyword.put_new(:producers, 16)
    |> Keyword.put_new(:workers, 16)
    |> Keyword.put_new(:create_batch, batch)
    |> Keyword.put_new(:create_mode, "many")
    |> Keyword.put_new(:create_inflight_batches, 2)
    |> Keyword.put_new(:create_auto_buckets, true)
    |> Keyword.put_new(:claim_batch, batch)
    |> Keyword.put_new(:claim_partition_batch, 16)
    |> Keyword.put_new(:claim_drain_batches, 2)
    |> Keyword.put_new(:complete_async_depth, 1)
    |> Keyword.put_new(:worker_connections, 16)
    |> Keyword.put_new(:server_shards, 16)
    |> Keyword.put_new(:timeout, 30_000)
  end
end

FerricStore.QueueBenchmark.run(System.argv())
