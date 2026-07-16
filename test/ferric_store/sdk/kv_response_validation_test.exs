defmodule FerricStore.SDK.KVResponseValidationTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.KV
  alias FerricStore.SDK.KV.{BatchResults, Response}
  alias FerricStore.SDK.Native.AdmissionGate
  alias FerricStore.Test.ClientRuntime

  defmodule ReplyClient do
    use GenServer

    def start_link(response),
      do: GenServer.start_link(__MODULE__, response) |> ClientRuntime.wrap()

    @impl true
    def init(response), do: {:ok, response}

    @impl true
    def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, response) do
      :ok = AdmissionGate.release(gate)
      handle_call(request, from, response)
    end

    def handle_call(
          {:command, _opcode, _key, _payload, _context},
          _from,
          {:delayed, delay_ms, response} = state
        ) do
      Process.sleep(delay_ms)
      {:reply, {:ok, response}, state}
    end

    def handle_call({:command, _opcode, _key, _payload, _context}, _from, response),
      do: {:reply, {:ok, response}, response}
  end

  test "a successful reply received after the absolute KV deadline is a timeout" do
    {:ok, client} = ReplyClient.start_link({:delayed, 20, "late"})

    assert {:error, :timeout} = KV.get(client, "key", timeout: 1)
  end

  test "typed scalar commands reject successful replies outside their server contract" do
    cases = [
      {:get, %{}, &KV.get(&1, "key")},
      {:set, "STORED", &KV.set(&1, "key", "value")},
      {:set, "OK", &KV.set(&1, "key", "value", nx: true)},
      {:set, true, &KV.set(&1, "key", "value", get: true)},
      {:cas, 1, &KV.cas(&1, "key", "old", "new")},
      {:lock, true, &KV.lock(&1, "key", "owner", 1_000)},
      {:unlock, 0, &KV.unlock(&1, "key", "owner")},
      {:extend, "1", &KV.extend(&1, "key", "owner", 1_000)},
      {:fetch_or_compute_result, "QUEUED",
       &KV.fetch_or_compute_result(&1, "key", "token", "value", 1_000)},
      {:fetch_or_compute_error, true, &KV.fetch_or_compute_error(&1, "key", "token", "failed")},
      {:hset, "OK", &KV.hset(&1, "key", %{"field" => "value"})},
      {:hget, 1, &KV.hget(&1, "key", "field")},
      {:lpush, -1, &KV.lpush(&1, "key", "value")},
      {:rpush, "1", &KV.rpush(&1, "key", "value")},
      {:sadd, false, &KV.sadd(&1, "key", "member")},
      {:srem, -1, &KV.srem(&1, "key", "member")},
      {:sismember, 1, &KV.sismember(&1, "key", "member")},
      {:zadd, -1, &KV.zadd(&1, "key", [{1.0, "member"}])},
      {:zrem, "1", &KV.zrem(&1, "key", "member")},
      {:zscore, 1.0, &KV.zscore(&1, "key", "member")},
      {:zscore, "1.0junk", &KV.zscore(&1, "key", "member")}
    ]

    for {operation, response, call} <- cases do
      {:ok, client} = ReplyClient.start_link(response)

      assert {:error, {:invalid_kv_response, %{operation: ^operation}}} = call.(client)
    end
  end

  test "structured KV replies are checked for exact shape and cardinality" do
    cases = [
      {:ratelimit_add, ["maybe", 1, 0, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10)},
      {:ratelimit_add, ["allowed", 11, 0, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10)},
      {:ratelimit_add, ["allowed", 2, 7, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10)},
      {:ratelimit_add, ["allowed", 1, 11, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10)},
      {:ratelimit_add, ["allowed", 1, 0, 1_001], &KV.ratelimit_add(&1, "rate", 1_000, 10)},
      {:ratelimit_add, ["allowed", 1, 9, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10, 2)},
      {:ratelimit_add, ["denied", 8, 2, 10], &KV.ratelimit_add(&1, "rate", 1_000, 10, 2)},
      {:fetch_or_compute, ["hit"], &KV.fetch_or_compute(&1, "cache", 1_000)},
      {:fetch_or_compute, ["hit", %{}], &KV.fetch_or_compute(&1, "cache", 1_000)},
      {:fetch_or_compute, ["compute", "hint", ""], &KV.fetch_or_compute(&1, "cache", 1_000)},
      {:hmget, ["only-one"], &KV.hmget(&1, "hash", ["first", "second"])},
      {:hmget, ["first", 2], &KV.hmget(&1, "hash", ["first", "second"])},
      {:hgetall, [], &KV.hgetall(&1, "hash")},
      {:hgetall, %{"field" => 1}, &KV.hgetall(&1, "hash")},
      {:lpop, "one", &KV.lpop(&1, "list", 2)},
      {:lpop, ["one", 2], &KV.lpop(&1, "list", 2)},
      {:lpop, ["one", "two", "three"], &KV.lpop(&1, "list", 2)},
      {:lpop, [], &KV.lpop(&1, "list", 2)},
      {:rpop, ["one"], &KV.rpop(&1, "list", 1)},
      {:lrange, %{}, &KV.lrange(&1, "list", 0, -1)},
      {:lrange, ["one", 2], &KV.lrange(&1, "list", 0, -1)},
      {:smembers, %{}, &KV.smembers(&1, "set")},
      {:smembers, [1], &KV.smembers(&1, "set")},
      {:zrange, %{}, &KV.zrange(&1, "zset", 0, -1)},
      {:zrange, ["member", "1.0", "dangling"], &KV.zrange(&1, "zset", 0, -1, withscores: true)},
      {:zrange, ["member", "not-a-score"], &KV.zrange(&1, "zset", 0, -1, withscores: true)}
    ]

    for {operation, response, call} <- cases do
      {:ok, client} = ReplyClient.start_link(response)

      assert {:error, {:invalid_kv_response, %{operation: ^operation}}} = call.(client)
    end
  end

  test "collection mutation counts cannot exceed the submitted item count" do
    cases = [
      {:hset, &KV.hset(&1, "hash", %{"field" => "value"})},
      {:sadd, &KV.sadd(&1, "set", "member")},
      {:srem, &KV.srem(&1, "set", "member")},
      {:zadd, &KV.zadd(&1, "zset", [{1.0, "member"}])},
      {:zrem, &KV.zrem(&1, "zset", "member")}
    ]

    for {operation, call} <- cases do
      assert {:error,
              {:invalid_kv_response,
               %{
                 operation: ^operation,
                 reason: :count_exceeds_input,
                 value: 2,
                 limit: 1
               }}} = call.(reply_client(2))
    end
  end

  test "scored zrange responses stop at the shared collection ceiling" do
    response = :lists.append(List.duplicate(["member", "1.0"], 100_001))
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, {:invalid_kv_response, %{operation: :zrange, reason: :too_many_items}}} =
             call(response, &KV.zrange(&1, "zset", 0, -1, withscores: true))

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 2_000_000
  end

  test "list pop responses cannot bypass the shared collection ceiling" do
    response = List.duplicate("value", 100_001)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, {:invalid_kv_response, %{operation: :lpop, reason: :too_many_items}}} =
             Response.pop({:ok, response}, :lpop, 200_000)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 1_000_000
  end

  test "valid current-protocol replies retain their useful values" do
    assert {:ok, true} = call(true, &KV.set(&1, "key", "value", nx: true))
    assert {:ok, false} = call(false, &KV.set(&1, "key", "value", nx: true))
    assert {:ok, "old"} = call("old", &KV.set(&1, "key", "value", get: true))
    assert {:ok, nil} = call(nil, &KV.set(&1, "key", "value", get: true))
    assert {:ok, "value"} = call("value", &KV.get(&1, "key"))
    assert {:ok, nil} = call(nil, &KV.get(&1, "missing"))
    assert {:ok, :ok} = call("OK", &KV.lock(&1, "key", "owner", 1_000))
    assert {:ok, true} = call(true, &KV.cas(&1, "key", "old", "new"))
    assert {:ok, nil} = call(nil, &KV.cas(&1, "key", "old", "new"))
    assert {:ok, 1} = call(1, &KV.unlock(&1, "key", "owner"))
    assert {:ok, 1} = call(1, &KV.hset(&1, "hash", %{"field" => "value"}))
    assert {:ok, "value"} = call("value", &KV.hget(&1, "hash", "field"))
    assert {:ok, nil} = call(nil, &KV.hget(&1, "hash", "missing"))
    assert {:ok, %{"field" => "value"}} = call(%{"field" => "value"}, &KV.hgetall(&1, "hash"))

    rate = ["allowed", 2, 8, 500]
    assert {:ok, ^rate} = call(rate, &KV.ratelimit_add(&1, "rate", 1_000, 10, 2))

    changed_limit = ["denied", 12, 0, 500]

    assert {:ok, ^changed_limit} =
             call(changed_limit, &KV.ratelimit_add(&1, "rate", 1_000, 10, 1))

    oversized_increment = ["denied", 0, 10, 500]

    assert {:ok, ^oversized_increment} =
             call(oversized_increment, &KV.ratelimit_add(&1, "rate", 1_000, 10, 11))

    compute = ["compute", "hint", "token"]
    assert {:ok, ^compute} = call(compute, &KV.fetch_or_compute(&1, "cache", 1_000))
    assert {:ok, ["a", nil]} = call(["a", nil], &KV.hmget(&1, "hash", ["a", "b"]))
    assert {:ok, ["a", "b"]} = call(["a", "b"], &KV.lrange(&1, "list", 0, -1))
    assert {:ok, ["a", "b"]} = call(["a", "b"], &KV.smembers(&1, "set"))
    assert {:ok, ["a", "b"]} = call(["a", "b"], &KV.lpop(&1, "list", 2))
    assert {:ok, nil} = call(nil, &KV.lpop(&1, "empty-list", 2))
    assert {:ok, nil} = call(nil, &KV.rpop(&1, "missing-list", 3))
    assert {:ok, "a"} = call("a", &KV.rpop(&1, "list", 1))

    assert {:ok, [{"a", 1.5}, {"b", -2.0}]} =
             call(["a", "1.5", "b", "-2"], &KV.zrange(&1, "zset", 0, -1, withscores: true))

    assert {:ok, 1.5} = call("1.5", &KV.zscore(&1, "zset", "member"))
    assert {:ok, nil} = call(nil, &KV.zscore(&1, "zset", "missing"))
  end

  test "mget validates each successful value without retaining malformed payloads" do
    secret = String.duplicate("invalid-mget-value", 10_000)

    assert {:error,
            {:invalid_mget_group_response, %{reason: :expected_binary_or_nil, index: 1} = details}} =
             BatchResults.mget(
               [%{indexes: [0, 1], value: ["valid", %{secret => secret}]}],
               2
             )

    refute inspect(details) =~ secret
  end

  test "malformed response errors do not retain or echo server payloads" do
    secret = String.duplicate("secret-response", 10_000)

    assert {:error, {:invalid_kv_response, details}} =
             call(secret, &KV.cas(&1, "key", "old", "new"))

    refute inspect(details) =~ "secret-response"
  end

  test "malformed grouped response errors do not retain server values" do
    secret = String.duplicate("grouped-secret", 10_000)

    results = [
      BatchResults.mget([%{indexes: [0], value: [secret | :invalid_tail]}], 1),
      BatchResults.mget(secret, 1),
      BatchResults.del([%{indexes: [0], value: secret}], 1),
      BatchResults.mset([%{indexes: [0], value: secret}], 1)
    ]

    Enum.each(results, fn result ->
      assert {:error, _reason} = result
      refute inspect(result) =~ "grouped-secret"
    end)
  end

  test "incomplete grouped responses report counts without materializing missing indexes" do
    assert {:error, {:missing_mget_indexes, %{actual_count: 1, expected_count: 100_000}}} =
             BatchResults.mget([%{indexes: [99_999], value: ["last"]}], 100_000)
  end

  defp call(response, fun) do
    fun.(reply_client(response))
  end

  defp reply_client(response) do
    {:ok, client} = ReplyClient.start_link(response)
    client
  end
end
