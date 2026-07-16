defmodule FerricStore.SDKHotPathBenchmark.GroupClient do
  @moduledoc false

  use GenServer

  alias FerricStore.ClientIdentity
  alias FerricStore.Protocol.PreparedMap
  alias FerricStore.SDK.Native.{AdmissionGate, KVPreparedRequest, Topology}

  @shard_count 16
  @slots_per_shard div(1_024, @shard_count)

  def start_link, do: GenServer.start_link(__MODULE__, nil)

  @impl true
  def init(nil) do
    endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    ClientIdentity.mark(:topology_aware, endpoint)
    {:ok, topology} = Topology.build(benchmark_topology_payload())
    submission_admission = AdmissionGate.new(1_024)

    true =
      :ets.insert(endpoint, [
        {:client, self()},
        {:coordinator, self()},
        {:topology, make_ref(), topology},
        {:submission_admission, submission_admission}
      ])

    {:ok, %{max_group_count: 0}}
  end

  @impl true
  def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, state) do
    :ok = AdmissionGate.release(gate)
    handle_call(request, from, state)
  end

  def handle_call({:kv_preparation_admission, _item_count, _context}, _from, state),
    do: {:reply, {:ok, make_ref()}, state}

  def handle_call(
        {:prepared_command_items,
         %KVPreparedRequest{operation: :mget, item_count: item_count, groups: groups}},
        _from,
        state
      ) do
    groups =
      groups
      |> Enum.reverse()
      |> Enum.map(fn group ->
        %{operation: :mget, items: keys} = PreparedMap.metadata(group.payload)
        Map.put(group, :value, keys)
      end)

    ^item_count = Enum.sum(Enum.map(groups, &length(&1.indexes)))

    state = %{state | max_group_count: max(state.max_group_count, length(groups))}
    {:reply, {:ok, groups}, state}
  end

  def handle_call(
        {:prepared_command_items,
         %KVPreparedRequest{operation: :mset, item_count: item_count, groups: groups}},
        _from,
        state
      ) do
    true =
      Enum.all?(groups, fn group ->
        not Map.has_key?(group, :items) and
          match?(
            %{operation: :mset, items: items} when is_list(items),
            PreparedMap.metadata(group.payload)
          )
      end)

    ^item_count = Enum.sum(Enum.map(groups, &length(&1.indexes)))
    groups = groups |> Enum.reverse() |> Enum.map(&Map.put(&1, :value, "OK"))

    state = %{state | max_group_count: max(state.max_group_count, length(groups))}
    {:reply, {:ok, groups}, state}
  end

  def handle_call(:max_group_count, _from, state),
    do: {:reply, state.max_group_count, state}

  defp benchmark_topology_payload do
    %{
      "route_epoch" => 1,
      "shard_count" => @shard_count,
      "ranges" =>
        Enum.map(0..(@shard_count - 1), fn shard ->
          first_slot = shard * @slots_per_shard

          %{
            "first_slot" => first_slot,
            "last_slot" => first_slot + @slots_per_shard - 1,
            "shard" => shard,
            "lane_id" => 1,
            "node" => "benchmark-#{shard}",
            "host" => "127.0.0.1",
            "native_port" => 6_388 + shard
          }
        end)
    }
  end
end

defmodule FerricStore.SDKHotPathBenchmark do
  @moduledoc false

  alias FerricStore.Protocol
  alias FerricStore.SDK.KV
  alias FerricStore.SDKHotPathBenchmark.GroupClient
  alias FerricStore.Transport.{FrameStream, RequestEncoder}

  def run(args) do
    opts = parse_args(args)
    iterations = opts[:iterations]
    frame_count = opts[:frames]
    body_bytes = opts[:body_bytes]
    packet_bytes = opts[:packet_bytes]
    key_count = opts[:keys]

    body = <<0::unsigned-16, Protocol.encode_value(:binary.copy("x", body_bytes))::binary>>
    frame = response_frame(body)
    wire = :binary.copy(frame, frame_count)
    packets = chunk_binary(wire, packet_bytes, [])

    {append_us, append_reductions, stream} =
      measure(iterations, fn ->
        Enum.reduce(packets, FrameStream.new(), &FrameStream.append(&2, &1))
      end)

    {decode_us, decode_reductions, decoded_count} =
      measure(iterations, fn -> drain_frames(stream, 0) end)

    {:ok, group_client} = GroupClient.start_link()

    keys = Enum.map(1..key_count, &"benchmark-key-#{&1}")

    mset_pairs = Enum.map(keys, &{&1, "benchmark-value"})

    {encode_us, encode_reductions, {:ok, encoded_request}} =
      measure(iterations, fn ->
        RequestEncoder.encode(
          Protocol.opcode(:mget),
          1,
          1,
          %{"keys" => keys},
          64 * 1024 * 1024
        )
      end)

    {:ok, _warm_values} = KV.mget(group_client, keys)

    {mget_us, mget_reductions, {:ok, values}} =
      measure(iterations, fn -> KV.mget(group_client, keys) end)

    {:ok, :ok} = KV.mset(group_client, mset_pairs, atomicity: :per_slot)

    {mset_us, mset_reductions, {:ok, mset_value}} =
      measure(iterations, fn ->
        KV.mset(group_client, mset_pairs, atomicity: :per_slot)
      end)

    max_group_count = GenServer.call(group_client, :max_group_count)
    GenServer.stop(group_client)

    unless decoded_count == frame_count and values == keys and mset_value == :ok and
             max_group_count > 1 and
             IO.iodata_length(encoded_request) > Protocol.header_size() do
      raise "hot-path benchmark produced an invalid result"
    end

    enforce_budget("request_encode", encode_us, encode_reductions, iterations,
      max_ms: opts[:max_encode_ms],
      max_reductions: opts[:max_encode_reductions]
    )

    enforce_budget("frame_append", append_us, append_reductions, iterations,
      max_ms: opts[:max_append_ms],
      max_reductions: opts[:max_append_reductions]
    )

    enforce_budget("frame_decode", decode_us, decode_reductions, iterations,
      max_ms: opts[:max_decode_ms],
      max_reductions: opts[:max_decode_reductions]
    )

    enforce_budget("mget_reorder", mget_us, mget_reductions, iterations,
      max_ms: opts[:max_mget_ms],
      max_reductions: opts[:max_mget_reductions]
    )

    enforce_budget("mset_prepare", mset_us, mset_reductions, iterations,
      max_ms: opts[:max_mset_ms],
      max_reductions: opts[:max_mset_reductions]
    )

    IO.puts(
      "frame_append iterations=#{iterations} packets=#{length(packets)} bytes=#{byte_size(wire)} " <>
        summary(append_us, append_reductions, iterations)
    )

    IO.puts(
      "frame_decode iterations=#{iterations} frames=#{frame_count} " <>
        summary(decode_us, decode_reductions, iterations)
    )

    IO.puts(
      "request_encode iterations=#{iterations} keys=#{key_count} " <>
        summary(encode_us, encode_reductions, iterations)
    )

    IO.puts(
      "mget_reorder iterations=#{iterations} keys=#{key_count} " <>
        summary(mget_us, mget_reductions, iterations)
    )

    IO.puts(
      "mset_prepare iterations=#{iterations} pairs=#{key_count} " <>
        summary(mset_us, mset_reductions, iterations)
    )
  end

  defp drain_frames(stream, count) do
    case FrameStream.next(stream, 64 * 1024 * 1024) do
      {:ok, _header, _body, rest} ->
        drain_frames(rest, count + 1)

      :incomplete ->
        if count > 0 and FrameStream.empty?(stream),
          do: count,
          else: raise("incomplete frame stream")

      other ->
        raise "frame decode failed: #{inspect(other)}"
    end
  end

  defp chunk_binary("", _packet_bytes, acc), do: Enum.reverse(acc)

  defp chunk_binary(binary, packet_bytes, acc) do
    size = min(byte_size(binary), packet_bytes)
    <<packet::binary-size(^size), rest::binary>> = binary
    chunk_binary(rest, packet_bytes, [packet | acc])
  end

  defp response_frame(body) do
    <<"FSNP", 0x81, 0, 1::unsigned-32, 0x0101::unsigned-16, 1::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
  end

  defp measure(iterations, fun) do
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    started = System.monotonic_time(:microsecond)

    result = Enum.reduce(1..iterations, nil, fn _, _previous -> fun.() end)

    elapsed_us = System.monotonic_time(:microsecond) - started
    {:reductions, after_reductions} = Process.info(self(), :reductions)
    {elapsed_us, after_reductions - before_reductions, result}
  end

  defp summary(elapsed_us, reductions, iterations) do
    per_iteration_us = elapsed_us / iterations
    reductions_per_iteration = reductions / iterations

    "elapsed_ms=#{Float.round(elapsed_us / 1_000, 3)} " <>
      "avg_ms=#{Float.round(per_iteration_us / 1_000, 3)} " <>
      "reductions_per_iteration=#{Float.round(reductions_per_iteration, 1)}"
  end

  defp enforce_budget(name, elapsed_us, reductions, iterations, opts) do
    average_ms = elapsed_us / iterations / 1_000
    average_reductions = reductions / iterations
    max_ms = Keyword.get(opts, :max_ms)
    max_reductions = Keyword.get(opts, :max_reductions)

    cond do
      is_number(max_ms) and average_ms > max_ms ->
        raise "#{name} latency regression: avg_ms=#{average_ms} budget_ms=#{max_ms}"

      is_number(max_reductions) and average_reductions > max_reductions ->
        raise "#{name} reduction regression: reductions=#{average_reductions} budget=#{max_reductions}"

      true ->
        :ok
    end
  end

  defp parse_args(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          iterations: :integer,
          frames: :integer,
          body_bytes: :integer,
          packet_bytes: :integer,
          keys: :integer,
          max_append_ms: :float,
          max_append_reductions: :integer,
          max_decode_ms: :float,
          max_decode_reductions: :integer,
          max_encode_ms: :float,
          max_encode_reductions: :integer,
          max_mget_ms: :float,
          max_mget_reductions: :integer,
          max_mset_ms: :float,
          max_mset_reductions: :integer
        ]
      )

    opts
    |> Keyword.put_new(:iterations, 5)
    |> Keyword.put_new(:frames, 10_000)
    |> Keyword.put_new(:body_bytes, 32)
    |> Keyword.put_new(:packet_bytes, 1_337)
    |> Keyword.put_new(:keys, 10_000)
  end
end

FerricStore.SDKHotPathBenchmark.run(System.argv())
