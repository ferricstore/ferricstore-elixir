defmodule FerricStore.SDK.KVTest do
  use ExUnit.Case, async: true

  alias FerricStore.{RequestContext, RequestLimits}
  alias FerricStore.SDK.KV
  alias FerricStore.SDK.KV.{BatchResults, Input, Options}
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    alias FerricStore.SDK.Native.AdmissionGate

    def start_link(owner, response_fun \\ &default_response/2),
      do:
        GenServer.start_link(__MODULE__, {owner, response_fun})
        |> ClientRuntime.wrap()

    @impl true
    def init({owner, response_fun}), do: {:ok, %{owner: owner, response_fun: response_fun}}

    @impl true
    def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, state) do
      :ok = AdmissionGate.release(gate)
      handle_call(request, from, state)
    end

    @impl true
    def handle_call({:route, key}, _from, state) do
      send(state.owner, {:route, key})
      {:reply, {:ok, %{shard: 0}}, state}
    end

    def handle_call(
          {:command_items, opcode, items, item_count, _key_fun, _payload_builder, opts},
          _from,
          state
        ) do
      send(state.owner, {:command_items, items, FerricStore.RequestContext.options(opts)})
      send(state.owner, {:command_item_count, item_count})
      {:reply, state.response_fun.(opcode, items), state}
    end

    def handle_call(
          {:command_items, opcode, items, _key_fun, _payload_builder, opts},
          _from,
          state
        ) do
      send(state.owner, {:command_items, items, FerricStore.RequestContext.options(opts)})
      {:reply, state.response_fun.(opcode, items), state}
    end

    def handle_call({:command, opcode, key, payload, opts}, _from, state) do
      send(
        state.owner,
        {:command, opcode, key, payload, FerricStore.RequestContext.options(opts)}
      )

      {:reply, {:ok, scalar_response(opcode, payload)}, state}
    end

    defp scalar_response(0x0106, _payload), do: true
    defp scalar_response(0x0107, _payload), do: "OK"
    defp scalar_response(opcode, _payload) when opcode in [0x0108, 0x0109], do: 1
    defp scalar_response(0x010A, _payload), do: ["allowed", 2, 8, 500]
    defp scalar_response(0x010B, _payload), do: ["compute", "hint", "token"]
    defp scalar_response(opcode, _payload) when opcode in [0x010C, 0x010D], do: "OK"
    defp scalar_response(0x0110, _payload), do: 1

    defp scalar_response(0x0112, %{"fields" => fields}),
      do: Enum.map(fields, fn _field -> nil end)

    defp scalar_response(0x0113, _payload), do: %{}
    defp scalar_response(opcode, _payload) when opcode in [0x0120, 0x0121], do: 1

    defp scalar_response(opcode, %{"count" => 1}) when opcode in [0x0122, 0x0123],
      do: "value"

    defp scalar_response(opcode, %{"count" => count}) when opcode in [0x0122, 0x0123],
      do: List.duplicate("value", count)

    defp scalar_response(0x0124, _payload), do: []
    defp scalar_response(opcode, _payload) when opcode in [0x0130, 0x0131], do: 1
    defp scalar_response(0x0132, _payload), do: []
    defp scalar_response(0x0133, _payload), do: true
    defp scalar_response(opcode, _payload) when opcode in [0x0140, 0x0141], do: 1
    defp scalar_response(0x0142, _payload), do: []
    defp scalar_response(0x0143, _payload), do: "1.0"
    defp scalar_response(_opcode, _payload), do: "OK"

    defp default_response(_opcode, items) do
      indexes = if items == [], do: [], else: Enum.to_list(0..(length(items) - 1))
      {:ok, [%{indexes: indexes, items: items, value: "OK"}]}
    end
  end

  test "same-slot writes validate and group in one client call" do
    {:ok, client} = CaptureClient.start_link(self())
    pairs = Enum.map(1..100, &{"key-#{&1}", "value"})

    assert {:ok, :ok} = KV.mset(client, pairs)
    assert_received {:command_items, ^pairs, opts}
    assert opts[:require_same_slot] == :mset
    refute_received {:route, _key}
  end

  test "mget reconstructs cross-group values in original key order" do
    response = fn _opcode, _items ->
      {:ok,
       [
         %{indexes: [1, 3], items: ["b", "d"], value: ["B", nil]},
         %{indexes: [0, 2], items: ["a", "c"], value: ["A", "C"]}
       ]}
    end

    {:ok, client} = CaptureClient.start_link(self(), response)
    assert {:ok, ["A", "B", "C", nil]} = KV.mget(client, ["a", "b", "c", "d"])
    assert_received {:command_item_count, 4}
  end

  test "mget rejects duplicate, missing, and out-of-range group indexes" do
    cases = [
      {[%{indexes: [0, 0], value: ["A", "again"]}], :duplicate_mget_index},
      {[%{indexes: [0], value: ["A"]}], :missing_mget_indexes},
      {[%{indexes: [0, 2], value: ["A", "outside"]}], :invalid_mget_index}
    ]

    for {groups, reason} <- cases do
      {:ok, client} = CaptureClient.start_link(self(), fn _opcode, _items -> {:ok, groups} end)

      assert {:error, {^reason, _details}} = KV.mget(client, ["a", "b"])
    end
  end

  test "malformed mget responses do not echo retained input values" do
    secret = "never-echo-this-input"

    {:ok, client} =
      CaptureClient.start_link(self(), fn _opcode, _items ->
        {:ok, [%{indexes: [0, 1], items: [secret, "other"], value: ["only-one"]}]}
      end)

    assert {:error, {:mismatched_mget_response, details}} =
             KV.mget(client, ["first", "second"])

    refute Map.has_key?(details, :items)
    refute inspect(details) =~ secret
  end

  test "grouped writes reject malformed success values" do
    {:ok, del_client} =
      CaptureClient.start_link(self(), fn _opcode, _items ->
        {:ok, [%{indexes: [0], value: 1}, %{indexes: [1], value: "one"}]}
      end)

    assert {:error, {:invalid_del_group_response, %{reason: :unexpected_value}}} =
             KV.del(del_client, ["a", "b"], atomicity: :per_shard)

    {:ok, mset_client} =
      CaptureClient.start_link(self(), fn _opcode, _items ->
        {:ok, [%{indexes: [0], value: "QUEUED"}]}
      end)

    assert {:error, {:invalid_mset_group_response, %{reason: :unexpected_value}}} =
             KV.mset(mset_client, [{"a", "A"}])
  end

  test "KV batch commands reject malformed top-level success responses" do
    cases = [
      {:mget, :invalid, fn client -> KV.mget(client, ["a"]) end},
      {:del, :invalid, fn client -> KV.del(client, ["a"]) end},
      {:mset, :invalid, fn client -> KV.mset(client, [{"a", "A"}]) end}
    ]

    Enum.each(cases, fn {operation, response, call} ->
      {:ok, client} =
        CaptureClient.start_link(self(), fn _opcode, _items -> {:ok, response} end)

      error_tag = String.to_atom("invalid_#{operation}_group_response")

      assert {:error, {^error_tag, %{reason: :unexpected_group_shape}}} = call.(client)
    end)
  end

  test "grouped result reconstruction rejects improper response lists without raising" do
    mget_groups = [%{indexes: [0], value: ["A"]} | :invalid_tail]
    del_groups = [%{indexes: [0], value: 1} | :invalid_tail]
    mset_groups = [%{indexes: [0], value: "OK"} | :invalid_tail]

    assert {:error, {:invalid_mget_group_response, %{reason: :unexpected_group_shape}}} =
             BatchResults.mget(mget_groups, 2)

    assert {:error, {:invalid_del_group_response, %{reason: :unexpected_group_shape}}} =
             BatchResults.del(del_groups, 2)

    assert {:error, {:invalid_mset_group_response, %{reason: :unexpected_group_shape}}} =
             BatchResults.mset(mset_groups, 2)
  end

  test "grouped result reconstruction rejects expected counts outside the SDK limit" do
    invalid_count = 100_001

    calls = [
      {:mget, fn -> BatchResults.mget([], invalid_count) end},
      {:del, fn -> BatchResults.del([], invalid_count) end},
      {:mset, fn -> BatchResults.mset([], invalid_count) end}
    ]

    for {operation, call} <- calls do
      assert {:error,
              {:invalid_kv_result_count,
               %{operation: ^operation, value: ^invalid_count, limit: 100_000}}} = call.()
    end
  end

  test "grouped result reconstruction rejects improper inner lists without raising" do
    improper_indexes = [0 | :invalid_tail]
    improper_values = ["A" | :invalid_tail]

    assert {:error, {:invalid_mget_group_response, %{reason: :improper_indexes}}} =
             BatchResults.mget([%{indexes: improper_indexes, value: ["A"]}], 1)

    assert {:error, {:invalid_mget_group_response, %{reason: :improper_values}}} =
             BatchResults.mget([%{indexes: [0], value: improper_values}], 1)

    assert {:error, {:invalid_mget_group_response, %{reason: :improper_indexes}}} =
             BatchResults.mget([%{indexes: improper_indexes, value: []}], 1)

    assert {:error, {:invalid_mget_group_response, %{reason: :improper_values}}} =
             BatchResults.mget([%{indexes: [], value: improper_values}], 1)

    assert {:error, {:invalid_del_group_response, %{reason: :improper_indexes}}} =
             BatchResults.del([%{indexes: improper_indexes, value: 1}], 1)

    assert {:error, {:invalid_mset_group_response, %{reason: :improper_indexes}}} =
             BatchResults.mset([%{indexes: improper_indexes, value: "OK"}], 1)
  end

  test "grouped result reconstruction rejects explicit empty groups" do
    assert {:error, {:invalid_mget_group_response, %{reason: :empty_indexes}}} =
             BatchResults.mget([%{indexes: [], value: []}], 0)

    assert {:error, {:invalid_mget_group_response, %{reason: :empty_indexes}}} =
             BatchResults.mget(
               [%{indexes: [], value: []}, %{indexes: [0], value: ["A"]}],
               1
             )

    assert {:error, {:invalid_del_group_response, %{reason: :empty_indexes}}} =
             BatchResults.del([%{indexes: [], value: 0}], 0)

    assert {:error, {:invalid_mset_group_response, %{reason: :empty_indexes}}} =
             BatchResults.mset([%{indexes: [], value: "OK"}], 0)
  end

  test "grouped writes reject incomplete and impossible success responses" do
    {:ok, del_client} =
      CaptureClient.start_link(self(), fn _opcode, _items ->
        {:ok, [%{indexes: [0], value: 2}]}
      end)

    assert {:error,
            {:invalid_del_group_response,
             %{group_items: 1, reason: :count_exceeds_group_items, value: 2}}} =
             KV.del(del_client, ["a", "b"], atomicity: :per_shard)

    {:ok, mset_client} =
      CaptureClient.start_link(self(), fn _opcode, _items ->
        {:ok, [%{indexes: [0], value: "OK"}]}
      end)

    assert {:error,
            {:invalid_mset_group_response,
             %{actual_items: 1, expected_items: 2, reason: :incomplete_groups}}} =
             KV.mset(mset_client, [{"a", "A"}, {"b", "B"}], atomicity: :per_slot)
  end

  test "grouped writes reject duplicate and out-of-range indexes" do
    cases = [
      {:del, [%{indexes: [0, 0], value: 2}], :duplicate_index},
      {:del, [%{indexes: [0, 2], value: 1}], :invalid_index},
      {:mset, [%{indexes: [0, 0], value: "OK"}], :duplicate_index},
      {:mset, [%{indexes: [0, 2], value: "OK"}], :invalid_index}
    ]

    Enum.each(cases, fn {operation, groups, reason} ->
      {:ok, client} =
        CaptureClient.start_link(self(), fn _opcode, _items -> {:ok, groups} end)

      result =
        case operation do
          :del -> KV.del(client, ["a", "b"], atomicity: :per_shard)
          :mset -> KV.mset(client, [{"a", "A"}, {"b", "B"}], atomicity: :per_slot)
        end

      assert {:error, {error_tag, %{reason: ^reason}}} = result
      assert error_tag == String.to_atom("invalid_#{operation}_group_response")
    end)
  end

  test "mget reconstruction scales linearly with key count" do
    response = fn _opcode, items ->
      indexes = items |> length() |> then(&Enum.to_list(0..(&1 - 1)))
      {:ok, [%{indexes: indexes, items: items, value: items}]}
    end

    {:ok, client} = CaptureClient.start_link(self(), response)
    assert {:ok, _values} = KV.mget(client, keys(100))

    small = measured_reductions(fn -> KV.mget(client, keys(1_000)) end)
    large = measured_reductions(fn -> KV.mget(client, keys(2_000)) end)

    assert large < small * 3
  end

  test "mget reconstruction keeps dense maximum-size batches within its reduction budget" do
    response = fn _opcode, items ->
      indexes = items |> length() |> then(&Enum.to_list(0..(&1 - 1)))
      {:ok, [%{indexes: indexes, items: items, value: items}]}
    end

    {:ok, client} = CaptureClient.start_link(self(), response)
    batch_keys = keys(100_000)
    assert {:ok, _values} = KV.mget(client, keys(100))

    assert measured_reductions(fn -> KV.mget(client, batch_keys) end) < 450_000
  end

  test "mget reconstruction stops at the first duplicate in an oversized malformed response" do
    indexes = List.duplicate(0, 100_000)
    values = List.duplicate("A", 100_000)
    :erlang.garbage_collect(self())

    {reductions, result} =
      count_reductions(fn ->
        BatchResults.mget([%{indexes: indexes, value: values}], 1)
      end)

    assert {:error, {:duplicate_mget_index, %{index: 0}}} = result
    assert reductions < 20_000
  end

  test "oversized mset input is bounded before pair normalization" do
    pairs = List.duplicate({"key", "value"}, 1_000_000)
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             KV.mset(self(), pairs)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000_000

    invalid_tail = List.duplicate({"key", "value"}, 100_001) ++ [:invalid_pair]

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             KV.mset(self(), invalid_tail)
  end

  test "mset maps preserve their O(1) cardinality source until admission" do
    pairs = Map.new(1..100_000, &{"key-#{&1}", &1})
    :erlang.garbage_collect(self())

    context = FerricStore.RequestContext.new([timeout: :infinity], 5_000)

    assert {:ok, %{"warmup" => true}, 1} =
             Input.mset_pairs(%{"warmup" => true}, RequestContext.budget(context))

    :erlang.garbage_collect(self())

    {reductions, result} =
      count_reductions(fn -> Input.mset_pairs(pairs, RequestContext.budget(context)) end)

    assert {:ok, ^pairs, 100_000} = result
    assert reductions < 1_000
  end

  test "improper collection lists return typed errors without reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error,
            {:invalid_kv_input,
             %{operation: :mget, field: :keys, reason: :improper_list, index: 1}}} =
             KV.mget(client, ["key" | :tail])

    assert {:error,
            {:invalid_kv_input,
             %{operation: :del, field: :keys, reason: :improper_list, index: 1}}} =
             KV.del(client, ["key" | :tail])

    assert {:error,
            {:invalid_kv_input,
             %{operation: :lpush, field: :values, reason: :improper_list, index: 1}}} =
             KV.lpush(client, "key", ["value" | :tail])

    assert {:error, {:invalid_mset_pairs, :improper_list}} =
             KV.mset(client, [{"key", "value"} | :tail])

    assert {:error,
            {:invalid_kv_input, %{operation: :zadd, field: :items, reason: :improper_list}}} =
             KV.zadd(client, "key", [{1.0, "member"} | :tail])

    refute_received {:command, _opcode, _key, _payload, _opts}
    refute_received {:command_items, _items, _opts}
  end

  test "empty mset containers are representation-independent no-ops" do
    assert {:ok, :ok} = KV.mset(self(), [])
    assert {:ok, :ok} = KV.mset(self(), %{})
  end

  test "zadd rejects an oversized input before normalizing any item" do
    {:ok, client} = CaptureClient.start_link(self())
    items = List.duplicate({1.0, "member"}, 100_001) ++ [:invalid_tail]

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             KV.zadd(client, "scores", items)

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "list pops reject counts above the shared collection ceiling before submission" do
    {:ok, client} = CaptureClient.start_link(self())
    count = RequestLimits.max_batch_items() + 1

    for call <- [
          fn -> KV.lpop(client, "list", count) end,
          fn -> KV.rpop(client, "list", count) end
        ] do
      assert {:error, {:batch_too_large, %{items: ^count, limit: 100_000}}} = call.()
    end

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "invalid zadd items return a typed error without reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error, {:invalid_zadd_item, :invalid}} =
             KV.zadd(client, "scores", [{1.0, "valid"}, :invalid])

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "zadd map items reject unexpected fields instead of silently discarding them" do
    {:ok, client} = CaptureClient.start_link(self())

    items = [
      %{"score" => 1.0, "member" => "a", "memeber" => "typo"},
      %{score: 2.0, member: "b", unexpected: true}
    ]

    Enum.each(items, fn item ->
      assert {:error, {:invalid_zadd_item, ^item}} = KV.zadd(client, "scores", [item])
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "hset rejects non-binary fields before reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error,
            {:invalid_kv_input,
             %{
               operation: :hset,
               field: :fields,
               reason: :expected_binary_field
             }}} = KV.hset(client, "hash", %{:field => 1, "field" => 2})

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "zadd rejects scores outside the signed 64-bit wire domain before reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())
    score = 9_223_372_036_854_775_808

    assert {:error, {:invalid_zadd_item, [^score, "member"]}} =
             KV.zadd(client, "scores", [[score, "member"]])

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "zadd rejects integer scores that lose precision in the server float domain" do
    {:ok, client} = CaptureClient.start_link(self())
    exact = 9_007_199_254_740_992
    lossy = exact + 1
    larger_exact = exact + 2

    assert {:ok, 1} =
             KV.zadd(client, "scores", [{exact, "exact"}, {larger_exact, "larger"}])

    assert_received {:command, _opcode, "scores",
                     %{"items" => [[^exact, "exact"], [^larger_exact, "larger"]]}, _opts}

    assert {:error, {:invalid_zadd_item, {^lossy, "lossy"}}} =
             KV.zadd(client, "scores", [{lossy, "lossy"}])

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "scalar integers outside the signed 64-bit wire domain fail locally" do
    {:ok, client} = CaptureClient.start_link(self())
    below_min = -9_223_372_036_854_775_809
    above_max = 9_223_372_036_854_775_808

    cases = [
      {:lrange, :start, below_min, fn -> KV.lrange(client, "k", below_min, -1) end},
      {:zrange, :stop, above_max, fn -> KV.zrange(client, "k", 0, above_max) end},
      {:lock, :ttl_ms, above_max, fn -> KV.lock(client, "k", "owner", above_max) end},
      {:ratelimit_add, :max, above_max, fn -> KV.ratelimit_add(client, "k", 1, above_max) end}
    ]

    Enum.each(cases, fn {operation, field, _value, call} ->
      assert {:error,
              {:invalid_kv_input,
               %{
                 operation: ^operation,
                 field: ^field,
                 reason: :outside_signed_64_domain
               }}} = call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "set and cas option grammar fails locally with typed errors" do
    {:ok, client} = CaptureClient.start_link(self())
    above_max = 9_223_372_036_854_775_808

    cases = [
      {:set, :ttl, :expected_non_negative_integer, -1,
       fn -> KV.set(client, "k", "v", ttl: -1) end},
      {:set, :ttl, :outside_signed_64_domain, above_max,
       fn -> KV.set(client, "k", "v", ttl: above_max) end},
      {:set, :exat, :expected_positive_integer, 0, fn -> KV.set(client, "k", "v", exat: 0) end},
      {:set, :pxat, :outside_signed_64_domain, above_max,
       fn -> KV.set(client, "k", "v", pxat: above_max) end},
      {:set, :nx, :expected_boolean, :yes, fn -> KV.set(client, "k", "v", nx: :yes) end},
      {:set, :xx, :expected_boolean, :yes, fn -> KV.set(client, "k", "v", xx: :yes) end},
      {:set, :get, :expected_boolean, :yes, fn -> KV.set(client, "k", "v", get: :yes) end},
      {:set, :keepttl, :expected_boolean, :yes,
       fn -> KV.set(client, "k", "v", keepttl: :yes) end},
      {:cas, :ttl, :expected_non_negative_integer, "later",
       fn -> KV.cas(client, "k", "old", "new", ttl: "later") end},
      {:cas, :ttl, :expected_non_negative_integer, -1,
       fn -> KV.cas(client, "k", "old", "new", ttl: -1) end},
      {:cas, :ttl, :outside_signed_64_domain, above_max,
       fn -> KV.cas(client, "k", "old", "new", ttl: above_max) end}
    ]

    Enum.each(cases, fn {operation, field, reason, _value, call} ->
      assert {:error,
              {:invalid_kv_input, %{operation: ^operation, field: ^field, reason: ^reason}}} =
               call.()
    end)

    assert {:error,
            {:invalid_kv_input,
             %{
               operation: :set,
               field: :conditions,
               reason: :mutually_exclusive,
               options: [:nx, :xx]
             }}} = KV.set(client, "k", "v", nx: true, xx: true)

    for opts <- [
          [ttl: 1, exat: 2],
          [ttl: 1, pxat: 2],
          [ttl: 1, keepttl: true],
          [exat: 1, pxat: 2]
        ] do
      assert {:error,
              {:invalid_kv_input, %{operation: :set, field: :expiry, reason: :mutually_exclusive}}} =
               KV.set(client, "k", "v", opts)
    end

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "SET GET preserves a previous value equal to the ordinary success token" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:ok, :ok} = KV.set(client, "key", "value")
    assert {:ok, "OK"} = KV.set(client, "key", "value", get: true)

    assert_received {:command, _opcode, "key", %{"get" => true}, _opts}
  end

  test "KV option grammar produces one typed request context" do
    assert {:ok, %RequestContext{} = context} =
             Options.validate(:set, timeout: 100, ttl: 10, nx: true)

    assert RequestContext.options(context) == [timeout: 100, ttl: 10, nx: true]
    refute function_exported?(Options, :validate_set, 1)
    refute function_exported?(Options, :validate_cas, 1)
    refute function_exported?(Options, :validate_atomicity, 2)
  end

  test "unsupported and legacy KV options fail instead of being silently discarded" do
    {:ok, client} = CaptureClient.start_link(self())

    cases = [
      {:set, :ttl_ms, fn -> KV.set(client, "k", "v", ttl_ms: 1_000) end},
      {:set, :tttl, fn -> KV.set(client, "k", "v", tttl: 1_000) end},
      {:zrange, :with_scores, fn -> KV.zrange(client, "k", 0, -1, with_scores: true) end},
      {:get, :unknown, fn -> KV.get(client, "k", unknown: true) end}
    ]

    Enum.each(cases, fn {operation, option, call} ->
      assert {:error,
              {:invalid_kv_input,
               %{
                 operation: ^operation,
                 field: :options,
                 reason: :unsupported_options,
                 options: [^option]
               }}} = call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "empty no-op KV calls still validate request options" do
    {:ok, client} = CaptureClient.start_link(self())

    calls = [
      fn -> KV.del(client, [], timeout: -1) end,
      fn -> KV.mget(client, [], timeout: -1) end,
      fn -> KV.mset(client, [], timeout: -1) end,
      fn -> KV.zadd(client, "k", [], timeout: -1) end
    ]

    Enum.each(calls, fn call ->
      assert {:error, {:invalid_request_option, :timeout, -1}} = call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
    refute_received {:command_items, _items, _opts}
  end

  test "duplicate KV options are rejected instead of using the first value" do
    for {operation, opts, duplicates} <- [
          {:mget, [timeout: 10, timeout: 20], [:timeout]},
          {:mset, [atomicity: :per_slot, atomicity: nil], [:atomicity]},
          {:set, [nx: true, ttl: 10, nx: false, ttl: 20], [:nx, :ttl]}
        ] do
      assert {:error,
              {:invalid_kv_input,
               %{
                 operation: ^operation,
                 field: :options,
                 reason: :duplicate_options,
                 options: ^duplicates
               }}} = Options.validate(operation, opts)
    end
  end

  test "KV batch concurrency rejects resource-amplifying values" do
    assert {:ok, _context} = Options.validate(:mget, max_group_concurrency: 256)

    assert {:error, {:invalid_request_option, :max_group_concurrency, 257}} =
             Options.validate(:mget, max_group_concurrency: 257)
  end

  test "grouped write atomicity reports each operation's exact policy" do
    {:ok, client} = CaptureClient.start_link(self())

    calls = [
      {:del, :expected_per_shard, fn -> KV.del(client, ["a", "b"], atomicity: :typo) end},
      {:del, :expected_per_shard, fn -> KV.del(client, [], atomicity: :typo) end},
      {:mset, :expected_per_slot,
       fn -> KV.mset(client, [{"a", "A"}, {"b", "B"}], atomicity: :typo) end},
      {:mset, :expected_per_slot, fn -> KV.mset(client, [], atomicity: :typo) end}
    ]

    Enum.each(calls, fn {operation, expected_reason, call} ->
      assert {:error,
              {:invalid_kv_input,
               %{
                 operation: ^operation,
                 field: :atomicity,
                 reason: ^expected_reason,
                 value: :typo
               }}} = call.()
    end)

    refute_received {:command_items, _items, _opts}
  end

  test "MSET exposes the server's per-slot partial atomicity policy without a shard alias" do
    assert {:ok, context} = Options.validate(:mset, atomicity: :per_slot)
    assert RequestContext.options(context) == [atomicity: :per_slot]

    assert {:error,
            {:invalid_kv_input,
             %{
               operation: :mset,
               field: :atomicity,
               reason: :expected_per_slot,
               value: :per_shard
             }}} = Options.validate(:mset, atomicity: :per_shard)
  end

  test "scalar KV grammar constraints fail locally with typed errors" do
    {:ok, client} = CaptureClient.start_link(self())

    cases = [
      {:lock, :owner, :expected_binary, fn -> KV.lock(client, "k", :owner, 1) end},
      {:lock, :ttl_ms, :expected_positive_integer, fn -> KV.lock(client, "k", "owner", 0) end},
      {:unlock, :owner, :expected_binary, fn -> KV.unlock(client, "k", :owner) end},
      {:extend, :owner, :expected_binary, fn -> KV.extend(client, "k", :owner, 1) end},
      {:extend, :ttl_ms, :expected_positive_integer,
       fn -> KV.extend(client, "k", "owner", 0) end},
      {:ratelimit_add, :window_ms, :expected_positive_integer,
       fn -> KV.ratelimit_add(client, "k", 0, 1) end},
      {:ratelimit_add, :max, :expected_positive_integer,
       fn -> KV.ratelimit_add(client, "k", 1, 0) end},
      {:ratelimit_add, :count, :expected_positive_integer,
       fn -> KV.ratelimit_add(client, "k", 1, 1, 0) end},
      {:fetch_or_compute, :ttl_ms, :expected_positive_integer,
       fn -> KV.fetch_or_compute(client, "k", 0) end},
      {:fetch_or_compute, :hint, :expected_binary,
       fn -> KV.fetch_or_compute(client, "k", 1, hint: :not_binary) end},
      {:fetch_or_compute_result, :token, :expected_nonempty_binary,
       fn -> KV.fetch_or_compute_result(client, "k", "", "value", 1) end},
      {:fetch_or_compute_result, :ttl_ms, :expected_positive_integer,
       fn -> KV.fetch_or_compute_result(client, "k", "token", "value", 0) end},
      {:fetch_or_compute_error, :token, :expected_nonempty_binary,
       fn -> KV.fetch_or_compute_error(client, "k", "", "failed") end},
      {:fetch_or_compute_error, :message, :expected_binary,
       fn -> KV.fetch_or_compute_error(client, "k", "token", :failed) end},
      {:hget, :field, :expected_binary, fn -> KV.hget(client, "k", :field) end},
      {:lpop, :count, :expected_positive_integer, fn -> KV.lpop(client, "k", 0) end},
      {:rpop, :count, :expected_positive_integer, fn -> KV.rpop(client, "k", 0) end},
      {:lrange, :start, :expected_integer, fn -> KV.lrange(client, "k", 0.0, -1) end},
      {:lrange, :stop, :expected_integer, fn -> KV.lrange(client, "k", 0, -1.0) end},
      {:sismember, :member, :expected_binary, fn -> KV.sismember(client, "k", :member) end},
      {:zrange, :start, :expected_integer, fn -> KV.zrange(client, "k", 0.0, -1) end},
      {:zrange, :stop, :expected_integer, fn -> KV.zrange(client, "k", 0, -1.0) end},
      {:zrange, :withscores, :expected_boolean,
       fn -> KV.zrange(client, "k", 0, -1, withscores: :yes) end},
      {:zscore, :member, :expected_binary, fn -> KV.zscore(client, "k", :member) end}
    ]

    Enum.each(cases, fn {operation, field, reason, call} ->
      assert {:error,
              {:invalid_kv_input, %{operation: ^operation, field: ^field, reason: ^reason}}} =
               call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "invalid mset containers return a typed error without reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error, {:invalid_mset_pairs, :invalid}} = KV.mset(client, :invalid)
    refute_received {:command_items, _items, _opts}
  end

  test "mset rejects pair maps with unexpected fields instead of silently discarding them" do
    {:ok, client} = CaptureClient.start_link(self())

    pairs = [
      %{"key" => "a", "value" => "A", "vale" => "typo"},
      %{key: "b", value: "B", unexpected: true}
    ]

    Enum.each(pairs, fn pair ->
      assert {:error, {:invalid_mset_pair, ^pair}} = KV.mset(client, [pair])
    end)

    refute_received {:command_items, _items, _opts}
  end

  test "invalid KV collection containers return typed errors without reaching the client" do
    {:ok, client} = CaptureClient.start_link(self())

    cases = [
      {:del, :keys, :expected_binary_or_list, fn -> KV.del(client, :invalid) end},
      {:mget, :keys, :expected_list, fn -> KV.mget(client, :invalid) end},
      {:hset, :fields, :expected_map, fn -> KV.hset(client, "k", :invalid) end},
      {:hmget, :fields, :expected_list, fn -> KV.hmget(client, "k", :invalid) end},
      {:lpush, :values, :expected_binary_or_list, fn -> KV.lpush(client, "k", %{}) end},
      {:rpush, :values, :expected_binary_or_list, fn -> KV.rpush(client, "k", %{}) end},
      {:sadd, :members, :expected_binary_or_list, fn -> KV.sadd(client, "k", %{}) end},
      {:srem, :members, :expected_binary_or_list, fn -> KV.srem(client, "k", %{}) end},
      {:zadd, :items, :expected_list, fn -> KV.zadd(client, "k", :invalid) end},
      {:zrem, :members, :expected_binary_or_list, fn -> KV.zrem(client, "k", %{}) end}
    ]

    Enum.each(cases, fn {operation, field, reason, call} ->
      assert {:error,
              {:invalid_kv_input, %{operation: ^operation, field: ^field, reason: ^reason}}} =
               call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
    refute_received {:command_items, _items, _opts}
  end

  test "empty KV collections rejected by the server fail locally without wire work" do
    {:ok, client} = CaptureClient.start_link(self())

    cases = [
      {:hset, :fields, fn -> KV.hset(client, "k", %{}) end},
      {:hmget, :fields, fn -> KV.hmget(client, "k", []) end},
      {:lpush, :values, fn -> KV.lpush(client, "k", []) end},
      {:rpush, :values, fn -> KV.rpush(client, "k", []) end},
      {:sadd, :members, fn -> KV.sadd(client, "k", []) end},
      {:srem, :members, fn -> KV.srem(client, "k", []) end},
      {:zrem, :members, fn -> KV.zrem(client, "k", []) end}
    ]

    Enum.each(cases, fn {operation, field, call} ->
      assert {:error,
              {:invalid_kv_input, %{operation: ^operation, field: ^field, reason: :empty}}} =
               call.()
    end)

    assert {:ok, 0} = KV.zadd(client, "k", [])
    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "single-key collections reject invalid route keys before scanning their input" do
    {:ok, client} = CaptureClient.start_link(self())
    invalid_key = :not_binary
    binaries = List.duplicate("value", 100_000)
    scores = List.duplicate({1.0, "member"}, 100_000)
    fields = Map.new(1..100_000, &{"field-#{&1}", &1})

    calls = [
      fn -> KV.hset(client, invalid_key, fields) end,
      fn -> KV.hmget(client, invalid_key, binaries) end,
      fn -> KV.lpush(client, invalid_key, binaries) end,
      fn -> KV.sadd(client, invalid_key, binaries) end,
      fn -> KV.zadd(client, invalid_key, scores) end
    ]

    assert {:error, {:invalid_route_key, ^invalid_key}} =
             KV.hmget(client, invalid_key, ["warmup"])

    for call <- calls do
      :erlang.garbage_collect(self())
      {reductions, result} = count_reductions(call)
      assert result == {:error, {:invalid_route_key, invalid_key}}
      assert reductions < 20_000
    end

    assert {:error, {:invalid_route_key, ^invalid_key}} = KV.zadd(client, invalid_key, [])
    refute_received {:command, _opcode, _key, _payload, _opts}
  end

  test "KV commands reject oversized route keys locally before wire work" do
    {:ok, client} = CaptureClient.start_link(self())
    oversized = :binary.copy("k", 65_536)
    error = {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}

    assert {:error, ^error} = KV.get(client, oversized)
    assert {:error, ^error} = KV.set(client, oversized, "value")
    assert {:error, ^error} = KV.hset(client, oversized, %{"field" => "value"})
    assert {:error, ^error} = KV.mget(client, [oversized])
    assert {:error, ^error} = KV.del(client, [oversized])
    assert {:error, ^error} = KV.mset(client, [{oversized, "value"}])

    refute_received {:command, _opcode, _key, _payload, _opts}
    refute_received {:command_items, _items, _opts}
  end

  test "invalid binary collection items fail locally at their original index" do
    {:ok, client} = CaptureClient.start_link(self())

    cases = [
      {:del, :keys, fn -> KV.del(client, ["valid", :invalid]) end},
      {:mget, :keys, fn -> KV.mget(client, ["valid", :invalid]) end},
      {:hmget, :fields, fn -> KV.hmget(client, "k", ["valid", :invalid]) end},
      {:lpush, :values, fn -> KV.lpush(client, "k", ["valid", :invalid]) end},
      {:rpush, :values, fn -> KV.rpush(client, "k", ["valid", :invalid]) end},
      {:sadd, :members, fn -> KV.sadd(client, "k", ["valid", :invalid]) end},
      {:srem, :members, fn -> KV.srem(client, "k", ["valid", :invalid]) end},
      {:zrem, :members, fn -> KV.zrem(client, "k", ["valid", :invalid]) end}
    ]

    Enum.each(cases, fn {operation, field, call} ->
      assert {:error,
              {:invalid_kv_input,
               %{
                 operation: ^operation,
                 field: ^field,
                 reason: :expected_binary,
                 index: 1,
                 value: :invalid
               }}} = call.()
    end)

    refute_received {:command, _opcode, _key, _payload, _opts}
    refute_received {:command_items, _items, _opts}
  end

  test "zadd validates, normalizes, and counts its input within one reduction budget" do
    {:ok, client} = CaptureClient.start_link(self())
    items = List.duplicate({1.0, "member"}, 100_000)

    assert measured_reductions(fn -> KV.zadd(client, "scores", items) end) < 400_000
  end

  test "binary collection validation uses a countdown checkpoint budget" do
    items = List.duplicate("key", 100_000)
    context = RequestContext.new([timeout: :infinity], 5_000)
    :erlang.garbage_collect(self())

    {reductions, result} =
      count_reductions(fn ->
        Input.binary_list(items, :mget, :keys, RequestContext.budget(context))
      end)

    assert {:ok, ^items, 100_000} = result
    assert reductions < 150_000
  end

  test "single-key wrappers construct typed payloads through the routed client" do
    {:ok, client} = CaptureClient.start_link(self())

    calls = [
      fn -> KV.get(client, "k") end,
      fn -> KV.set(client, "k", "v") end,
      fn -> KV.cas(client, "k", "old", "new", ttl: 10) end,
      fn -> KV.lock(client, "k", "owner", 1_000) end,
      fn -> KV.unlock(client, "k", "owner") end,
      fn -> KV.extend(client, "k", "owner", 2_000) end,
      fn -> KV.ratelimit_add(client, "k", 1_000, 10, 2) end,
      fn -> KV.fetch_or_compute(client, "k", 5_000, hint: "work") end,
      fn -> KV.fetch_or_compute_result(client, "k", "token", "value", 5_000) end,
      fn -> KV.fetch_or_compute_error(client, "k", "token", "failed") end,
      fn -> KV.hset(client, "k", %{"field" => "value"}) end,
      fn -> KV.hget(client, "k", "field") end,
      fn -> KV.hmget(client, "k", ["a", "b"]) end,
      fn -> KV.hgetall(client, "k") end,
      fn -> KV.lpush(client, "k", ["a", "b"]) end,
      fn -> KV.rpush(client, "k", "a") end,
      fn -> KV.lpop(client, "k", 2) end,
      fn -> KV.rpop(client, "k", 2) end,
      fn -> KV.lrange(client, "k", 0, -1) end,
      fn -> KV.sadd(client, "k", ["a", "b"]) end,
      fn -> KV.srem(client, "k", "a") end,
      fn -> KV.smembers(client, "k") end,
      fn -> KV.sismember(client, "k", "a") end,
      fn -> KV.zadd(client, "k", [{1.5, "a"}]) end,
      fn -> KV.zrem(client, "k", "a") end,
      fn -> KV.zrange(client, "k", 0, -1, withscores: true) end,
      fn -> KV.zscore(client, "k", "a") end
    ]

    Enum.each(calls, fn call -> assert match?({:ok, _value}, call.()) end)

    messages = drain_commands([])
    assert length(messages) == length(calls)

    assert Enum.all?(messages, fn {:command, _opcode, "k", payload, _opts} ->
             payload["key"] == "k"
           end)
  end

  test "fetch-or-compute completion commands match the current server schemas" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:ok, :ok} =
             KV.fetch_or_compute_result(client, "k", "compute-token", "value", 5_000)

    assert_received {:command, _opcode, "k",
                     %{
                       "key" => "k",
                       "token" => "compute-token",
                       "value" => "value",
                       "ttl_ms" => 5_000
                     }, _opts}

    assert {:ok, :ok} =
             KV.fetch_or_compute_error(client, "k", "compute-token", "failed")

    assert_received {:command, _opcode, "k",
                     %{
                       "key" => "k",
                       "token" => "compute-token",
                       "message" => "failed"
                     }, _opts}
  end

  defp keys(count), do: Enum.map(1..count, &"key-#{&1}")

  defp measured_reductions(fun) do
    {reductions, result} = count_reductions(fun)
    assert {:ok, _values} = result
    reductions
  end

  defp count_reductions(fun) do
    {:reductions, before_count} = Process.info(self(), :reductions)
    result = fun.()
    {:reductions, after_count} = Process.info(self(), :reductions)
    {after_count - before_count, result}
  end

  defp drain_commands(acc) do
    receive do
      {:command, _opcode, _key, _payload, _opts} = command -> drain_commands([command | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
