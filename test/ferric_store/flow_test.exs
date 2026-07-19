defmodule FerricStore.FlowTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Flow
  alias FerricStore.Flow.PolicySnapshot
  alias FerricStore.Protocol
  alias FerricStore.Test.ClientRuntime

  defmodule CountingCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode(counter) do
      :atomics.add(counter, 1, 1)
      "encoded"
    end

    @impl true
    def decode(value), do: value
  end

  defmodule SlowCountingCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode(counter) do
      :atomics.add(counter, 1, 1)
      _sum = Enum.reduce(1..2_000, 0, fn value, sum -> value + sum end)
      "encoded"
    end

    @impl true
    def decode(value), do: value
  end

  defmodule SlowDecodeCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode(value), do: :erlang.term_to_binary(value)

    @impl true
    def decode(value) do
      {owner, delay, decoded} = :erlang.binary_to_term(value)
      send(owner, {:slow_flow_decoder, self()})
      Process.sleep(delay)
      decoded
    end
  end

  defmodule SlowEncodeCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode({owner, delay, encoded}) do
      send(owner, {:slow_flow_encoder, self()})
      Process.sleep(delay)
      encoded
    end

    @impl true
    def decode(value), do: value
  end

  defmodule CaptureNativeClient do
    use GenServer

    def start_link(test_pid),
      do:
        GenServer.start_link(__MODULE__, test_pid)
        |> ClientRuntime.wrap()

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call(
          {:command, 0x0100, key, %{"command" => command, "args" => args}, opts},
          _from,
          test_pid
        ) do
      send(test_pid, {:command_route, key})
      opts = opts |> FerricStore.RequestContext.options() |> Keyword.delete(:key)
      send(test_pid, {:command, command, args, opts})
      {:reply, {:ok, %{"type" => List.first(args)}}, test_pid}
    end

    def handle_call(
          {:request, 0x0100, %{"command" => command, "args" => args}, opts},
          _from,
          test_pid
        ) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:command, command, args, opts})
      {:reply, {:ok, %{"type" => List.first(args)}}, test_pid}
    end

    def handle_call({:command, opcode, key, payload, opts}, from, test_pid) do
      send(test_pid, {:native_route, key})
      handle_call({:request, opcode, payload, opts}, from, test_pid)
    end

    def handle_call({:request, opcode, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:native, opcode, payload, opts})

      reply = reply(opcode, payload)

      {:reply, {:ok, reply}, test_pid}
    end

    def handle_call({:native, opcode, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:native, opcode, payload, opts})

      reply = reply(opcode, payload)

      {:reply, reply, test_pid}
    end

    def handle_call({:command, command, args, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:command, command, args, opts})
      {:reply, %{"type" => List.first(args)}, test_pid}
    end

    defp reply(unquote(FerricStore.Protocol.opcode(:flow_claim_due)), _payload) do
      [
        ["flow-1", "tenant:a", "lease-1", 10],
        ["flow-2", "tenant:b", "lease-2", 11, %{"tenant" => "acme"}]
      ]
    end

    defp reply(unquote(FerricStore.Protocol.opcode(:flow_get)), payload),
      do: %{"id" => payload["id"]}

    defp reply(opcode, _payload)
         when opcode in [
                unquote(FerricStore.Protocol.opcode(:flow_list)),
                unquote(FerricStore.Protocol.opcode(:flow_history)),
                unquote(FerricStore.Protocol.opcode(:flow_search))
              ],
         do: []

    defp reply(opcode, payload)
         when opcode in [
                unquote(FerricStore.Protocol.opcode(:flow_policy_set)),
                unquote(FerricStore.Protocol.opcode(:flow_policy_get))
              ] do
      payload
      |> Map.drop(["expected_generation", "replace"])
      |> Map.merge(%{"generation" => 1, "states" => Map.get(payload, "states", %{})})
    end

    defp reply(_opcode, _payload), do: "OK"
  end

  defmodule CaptureReadClient do
    use GenServer

    def start_link(test_pid, record, values),
      do:
        GenServer.start_link(__MODULE__, {test_pid, record, values})
        |> ClientRuntime.wrap()

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call(
          {:command, 0x0100, _key, %{"command" => command, "args" => args}, opts},
          _from,
          {test_pid, record, _values} = state
        ) do
      opts = opts |> FerricStore.RequestContext.options() |> Keyword.delete(:key)
      send(test_pid, {:command, command, args, opts})
      {:reply, {:ok, record}, state}
    end

    def handle_call(
          {:command, opcode, key, payload, opts},
          _from,
          {test_pid, record, values} = state
        ) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:native_route, key})
      send(test_pid, {:native, opcode, payload, opts})

      reply =
        if opcode == FerricStore.Protocol.opcode(:flow_value_mget),
          do: values,
          else: record

      {:reply, {:ok, reply}, state}
    end

    def handle_call({:request, opcode, payload, opts}, _from, {test_pid, _record, values} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:native, opcode, payload, opts})
      {:reply, {:ok, values}, state}
    end

    def handle_call({:command, command, args, opts}, _from, {test_pid, record, _values} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:command, command, args, opts})
      {:reply, record, state}
    end

    def handle_call({:native, opcode, payload, opts}, _from, {test_pid, _record, values} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:native, opcode, payload, opts})
      {:reply, values, state}
    end
  end

  defmodule ClaimShapeClient do
    use GenServer

    def start_link(response),
      do:
        GenServer.start_link(__MODULE__, response)
        |> ClientRuntime.wrap()

    @impl true
    def init(response), do: {:ok, response}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({:request, _opcode, _payload, _opts}, _from, response),
      do: {:reply, {:ok, response}, response}

    def handle_call({:command, _opcode, _key, _payload, _opts}, _from, response),
      do: {:reply, {:ok, response}, response}

    def handle_call({:native, _opcode, _payload, _opts}, _from, response),
      do: {:reply, response, response}
  end

  test "partition-scoped flow commands route by the canonical server partition tag" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    partition = "tenant-route"
    digest = partition |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)
    route = {:slot, Bitwise.band(:erlang.crc32("f:#{digest}"), 1_023)}
    id = "flow-id"

    calls = [
      fn -> Flow.get(client, id, partition_key: partition) end,
      fn -> Flow.history(client, id, partition_key: partition) end,
      fn ->
        Flow.retry(client, id,
          partition_key: partition,
          lease_token: "lease",
          fencing_token: 1
        )
      end,
      fn ->
        Flow.fail(client, id,
          partition_key: partition,
          lease_token: "lease",
          fencing_token: 1
        )
      end,
      fn -> Flow.cancel(client, id, partition_key: partition, fencing_token: 1) end,
      fn -> Flow.signal(client, id, partition_key: partition, signal: "wake") end
    ]

    Enum.each(calls, fn call ->
      call.()
      assert_receive {:native_route, ^route}
      refute_received {:command, _command, _args, _opts}
    end)
  end

  test "legacy-prone Flow helpers use canonical typed payloads" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert [] =
             Flow.list(client,
               type: "review",
               state: "queued",
               partition_key: "tenant:a",
               count: 20,
               from_ms: 10,
               to_ms: 20,
               rev: true,
               attributes: %{tenant: "acme"},
               include_cold: true,
               consistent_projection: true
             )

    assert_received {:native, list_opcode,
                     %{
                       "type" => "review",
                       "state" => "queued",
                       "partition_key" => "tenant:a",
                       "count" => 20,
                       "from_ms" => 10,
                       "to_ms" => 20,
                       "rev" => true,
                       "attributes" => %{"tenant" => "acme"},
                       "include_cold" => true,
                       "consistent_projection" => true
                     }, []}

    assert list_opcode == Protocol.opcode(:flow_list)

    assert [] =
             Flow.history(client, "flow-1",
               partition_key: "tenant:a",
               count: 30,
               from_event: "1-0",
               to_event: "2-0",
               from_ms: 10,
               to_ms: 20,
               from_version: 1,
               to_version: 2,
               rev: true,
               event: "transitioned",
               worker: "worker-1",
               values: true,
               payload_max_bytes: 1_024,
               include_cold: false,
               consistent_projection: false
             )

    assert_received {:native, history_opcode,
                     %{
                       "id" => "flow-1",
                       "partition_key" => "tenant:a",
                       "count" => 30,
                       "from_event" => "1-0",
                       "to_event" => "2-0",
                       "from_ms" => 10,
                       "to_ms" => 20,
                       "from_version" => 1,
                       "to_version" => 2,
                       "rev" => true,
                       "event" => "transitioned",
                       "worker" => "worker-1",
                       "values" => true,
                       "payload_max_bytes" => 1_024,
                       "include_cold" => false,
                       "consistent_projection" => false
                     }, []}

    assert history_opcode == Protocol.opcode(:flow_history)

    assert "OK" =
             Flow.retry(client, "flow-1",
               partition_key: "tenant:a",
               lease_token: "lease-1",
               fencing_token: 7,
               error: "transient",
               payload: "next",
               run_at_ms: 50,
               retry: [max_retries: 5, exhausted_to: "failed"],
               attributes_merge: %{attempt: 2},
               attributes_delete: ["old"],
               state_meta: %{reason: "retry"},
               now_ms: 40
             )

    assert_received {:native, retry_opcode,
                     %{
                       "id" => "flow-1",
                       "partition_key" => "tenant:a",
                       "lease_token" => "lease-1",
                       "fencing_token" => 7,
                       "error" => "transient",
                       "payload" => "next",
                       "run_at_ms" => 50,
                       "retry" => %{"max_retries" => 5, "exhausted_to" => "failed"},
                       "attributes_merge" => %{"attempt" => 2},
                       "attributes_delete" => ["old"],
                       "state_meta" => %{"reason" => "retry"},
                       "now_ms" => 40
                     }, []}

    assert retry_opcode == Protocol.opcode(:flow_retry)

    assert "OK" =
             Flow.fail(client, "flow-1",
               lease_token: "lease-1",
               fencing_token: 7,
               error: "fatal",
               values: %{diagnostic: "details"},
               now_ms: 60
             )

    assert_received {:native, fail_opcode,
                     %{
                       "id" => "flow-1",
                       "lease_token" => "lease-1",
                       "fencing_token" => 7,
                       "error" => "fatal",
                       "values" => %{"diagnostic" => "details"},
                       "now_ms" => 60
                     }, []}

    assert fail_opcode == Protocol.opcode(:flow_fail)

    assert "OK" =
             Flow.cancel(client, "flow-1",
               fencing_token: 7,
               lease_token: "lease-1",
               reason: "operator",
               values: %{audit: "manual"},
               now_ms: 70
             )

    assert_received {:native, cancel_opcode,
                     %{
                       "id" => "flow-1",
                       "fencing_token" => 7,
                       "lease_token" => "lease-1",
                       "reason" => "operator",
                       "values" => %{"audit" => "manual"},
                       "now_ms" => 70
                     }, []}

    assert cancel_opcode == Protocol.opcode(:flow_cancel)

    assert "OK" =
             Flow.signal(client, "flow-1",
               signal: "wake",
               if_state: ["queued", "waiting"],
               transition_to: "ready",
               idempotency_key: "signal-1",
               values: %{input: "go"},
               run_at_ms: 90,
               now_ms: 80
             )

    assert_received {:native, signal_opcode,
                     %{
                       "id" => "flow-1",
                       "signal" => "wake",
                       "if_state" => ["queued", "waiting"],
                       "transition_to" => "ready",
                       "idempotency_key" => "signal-1",
                       "values" => %{"input" => "go"},
                       "run_at_ms" => 90,
                       "now_ms" => 80
                     }, []}

    assert signal_opcode == Protocol.opcode(:flow_signal)
    refute_received {:command, _command, _args, _opts}
  end

  test "unsupported mutation options fail before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :complete, [:error]}}} =
             Flow.complete(client, "flow-1", lease_token: "lease-1", error: "legacy")

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :fail, [:result]}}} =
             Flow.fail(client, "flow-1", lease_token: "lease-1", result: "legacy")

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :retry, [:values]}}} =
             Flow.retry(client, "flow-1",
               lease_token: "lease-1",
               fencing_token: 1,
               values: %{ignored: "bad"}
             )

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :signal, [:priority]}}} =
             Flow.signal(client, "flow-1", signal: "wake", priority: 2)

    refute_received {:native, _opcode, _payload, _opts}
    refute_received {:command, _command, _args, _opts}
  end

  test "Flow transport commands reject unknown, duplicate, and missing options" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :create, [:typo]}}} =
             Flow.create(client, "flow-1", type: "email", typo: true)

    assert {:error, %FerricStore.Error{raw: {:unsupported_flow_options, :get, [:result]}}} =
             Flow.get(client, "flow-1", result: "ignored")

    assert {:error, %FerricStore.Error{raw: {:duplicate_flow_options, :create, [:type]}}} =
             Flow.create(client, "flow-1", type: "email", type: "other")

    assert {:error, %FerricStore.Error{raw: {:missing_flow_options, :create, [:type]}}} =
             Flow.create(client, "flow-1", [])

    assert {:error,
            %FerricStore.Error{
              raw: {:missing_flow_options, :complete, [:fencing_token, :lease_token]}
            }} = Flow.complete(client, "flow-1", [])

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects unusable codecs before payload construction or response decoding" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    for {operation, call} <- [
          {:create,
           fn ->
             Flow.create(client, "flow-1", type: "email", payload: "body", codec: String)
           end},
          {:get, fn -> Flow.get(client, "flow-1", codec: :missing_flow_codec) end}
        ] do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, :codec, :expected_codec}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects map options that would crash partial payload normalizers" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    for {operation, option, call} <- [
          {:create, :attributes,
           fn -> Flow.create(client, "flow-1", type: "email", attributes: []) end},
          {:create, :values, fn -> Flow.create(client, "flow-1", type: "email", values: []) end},
          {:create, :value_refs,
           fn -> Flow.create(client, "flow-1", type: "email", value_refs: []) end},
          {:list, :attributes, fn -> Flow.list(client, type: "email", attributes: []) end},
          {:search, :state_meta, fn -> Flow.search(client, type: "email", state_meta: []) end}
        ] do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, :expected_map}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects non-boolean flag options before payload construction" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    cases = [
      {:get, :full, fn -> Flow.get(client, "flow-1", full: :yes) end},
      {:list, :rev, fn -> Flow.list(client, type: "email", rev: :yes) end},
      {:history, :include_cold, fn -> Flow.history(client, "flow-1", include_cold: :yes) end},
      {:claim_due, :include_state,
       fn -> Flow.claim_due(client, "email", worker: "worker", include_state: :yes) end},
      {:create, :idempotent,
       fn -> Flow.create(client, "flow-1", type: "email", idempotent: :yes) end},
      {:create_many, :idempotent,
       fn -> Flow.create_many(client, ["flow-1"], type: "email", idempotent: :yes) end},
      {:create_many, :independent,
       fn -> Flow.create_many(client, ["flow-1"], type: "email", independent: :yes) end},
      {:search, :terminal_only,
       fn -> Flow.search(client, type: "email", terminal_only: :yes) end},
      {:value_put, :override, fn -> Flow.value_put(client, "value", override: :yes) end}
    ]

    for {operation, option, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, :expected_boolean}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects malformed required scalar options before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    cases = [
      {:create, :type, :expected_nonempty_binary,
       fn -> Flow.create(client, "flow-1", type: nil) end},
      {:create_many, :type, :expected_nonempty_binary,
       fn -> Flow.create_many(client, ["flow-1"], type: :email) end},
      {:list, :type, :expected_nonempty_binary, fn -> Flow.list(client, type: "") end},
      {:claim_due, :worker, :expected_nonempty_binary,
       fn -> Flow.claim_due(client, "email", worker: nil) end},
      {:transition, :from_state, :expected_nonempty_binary,
       fn ->
         Flow.transition(client, "flow-1",
           from_state: nil,
           to_state: "running",
           lease_token: "lease",
           fencing_token: 1
         )
       end},
      {:complete, :lease_token, :expected_nonempty_binary,
       fn -> Flow.complete(client, "flow-1", lease_token: nil, fencing_token: 1) end},
      {:complete, :fencing_token, :expected_nonnegative_exact_integer,
       fn -> Flow.complete(client, "flow-1", lease_token: "lease", fencing_token: -1) end},
      {:signal, :signal, :expected_nonempty_binary,
       fn -> Flow.signal(client, "flow-1", signal: "") end}
    ]

    for {operation, option, expectation, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, ^expectation}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects numeric options outside server domains before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    above_exact = 9_007_199_254_740_992

    cases = [
      {:create, :now_ms, :expected_nonnegative_exact_integer,
       fn -> Flow.create(client, "flow-1", type: "email", now_ms: -1) end},
      {:create, :run_at_ms, :expected_nonnegative_exact_integer,
       fn -> Flow.create(client, "flow-1", type: "email", run_at_ms: above_exact) end},
      {:create, :priority, :expected_priority,
       fn -> Flow.create(client, "flow-1", type: "email", priority: 3) end},
      {:create, :retention_ttl_ms, :expected_positive_signed_64_integer,
       fn -> Flow.create(client, "flow-1", type: "email", retention_ttl_ms: 0) end},
      {:create, :max_active_ms, :expected_positive_bounded_duration_or_infinity,
       fn -> Flow.create(client, "flow-1", type: "email", max_active_ms: 0) end},
      {:create, :history_hot_max_events, :expected_history_hot_event_limit,
       fn -> Flow.create(client, "flow-1", type: "email", history_hot_max_events: 10_001) end},
      {:create, :history_max_events, :expected_history_event_limit,
       fn -> Flow.create(client, "flow-1", type: "email", history_max_events: 0) end},
      {:history, :count, :expected_positive_exact_integer,
       fn -> Flow.history(client, "flow-1", count: 0) end},
      {:history, :from_version, :expected_nonnegative_exact_integer,
       fn -> Flow.history(client, "flow-1", from_version: -1) end},
      {:claim_due, :lease_ms, :expected_positive_exact_integer,
       fn -> Flow.claim_due(client, "email", worker: "worker", lease_ms: 0) end},
      {:claim_due, :limit, :expected_positive_exact_integer,
       fn -> Flow.claim_due(client, "email", worker: "worker", limit: 0) end},
      {:claim_due, :block_ms, :expected_unsigned_32_integer,
       fn -> Flow.claim_due(client, "email", worker: "worker", block_ms: 0x1_0000_0000) end},
      {:claim_due, :reclaim_ratio, :expected_percentage_integer,
       fn -> Flow.claim_due(client, "email", worker: "worker", reclaim_ratio: 101) end},
      {:get, :payload_max_bytes, :expected_nonnegative_exact_integer,
       fn -> Flow.get(client, "flow-1", payload_max_bytes: -1) end},
      {:value_put, :ttl_ms, :expected_positive_exact_integer,
       fn -> Flow.value_put(client, "value", ttl_ms: 0) end},
      {:value_mget, :max_bytes, :expected_nonnegative_exact_integer,
       fn -> Flow.value_mget(client, ["ref"], max_bytes: -1) end},
      {:complete, :fencing_token, :expected_nonnegative_exact_integer,
       fn ->
         Flow.complete(client, "flow-1", lease_token: "lease", fencing_token: above_exact)
       end}
    ]

    for {operation, option, expectation, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, ^expectation}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects malformed string and selector options before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    oversized_ref = :binary.copy("x", 4_097)

    cases = [
      {:create, :state, :expected_nonempty_binary,
       fn -> Flow.create(client, "flow-1", type: "email", state: nil) end},
      {:create, :partition_key, :expected_partition_key,
       fn -> Flow.create(client, "flow-1", type: "email", partition_key: :auto) end},
      {:list, :partition_key, :expected_auto_partition_key,
       fn -> Flow.list(client, type: "email", partition_key: :global) end},
      {:history, :from_event, :expected_history_event_id,
       fn -> Flow.history(client, "flow-1", from_event: "1-x") end},
      {:history, :event, :expected_binary_or_nil,
       fn -> Flow.history(client, "flow-1", event: :created) end},
      {:cancel, :lease_token, :expected_nonempty_binary_or_nil,
       fn -> Flow.cancel(client, "flow-1", fencing_token: 1, lease_token: "") end},
      {:signal, :transition_to, :expected_binary_or_nil,
       fn -> Flow.signal(client, "flow-1", signal: "wake", transition_to: :queued) end},
      {:value_put, :owner_flow_id, :expected_nonempty_binary_or_nil,
       fn -> Flow.value_put(client, "value", owner_flow_id: "") end},
      {:create, :parent_flow_id, :expected_binary_or_nil,
       fn -> Flow.create(client, "flow-1", type: "email", parent_flow_id: :parent) end},
      {:create, :parent_flow_id, {:maximum_bytes, 4_096},
       fn -> Flow.create(client, "flow-1", type: "email", parent_flow_id: oversized_ref) end},
      {:signal, :idempotency_key, {:maximum_bytes, 4_096},
       fn -> Flow.signal(client, "flow-1", signal: "wake", idempotency_key: oversized_ref) end}
    ]

    for {operation, option, expectation, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, ^expectation}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects malformed collection domains before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    cases = [
      {:claim_due, :states, :expected_nonempty_state_list,
       fn -> Flow.claim_due(client, "email", worker: "worker", states: []) end},
      {:claim_due, :states, :expected_state_list,
       fn -> Flow.claim_due(client, "email", worker: "worker", states: ["queued", nil]) end},
      {:claim_due, :partition_keys, :expected_nonempty_partition_key_list,
       fn -> Flow.claim_due(client, "email", worker: "worker", partition_keys: nil) end},
      {:claim_due, :filters, {:maximum_filter_footprint, 64},
       fn ->
         Flow.claim_due(client, "email",
           worker: "worker",
           states: Enum.map(1..9, &"state-#{&1}"),
           partition_keys: Enum.map(1..8, &"partition-#{&1}")
         )
       end},
      {:signal, :if_state, :expected_state_or_state_list,
       fn -> Flow.signal(client, "flow-1", signal: "wake", if_state: ["queued", nil]) end},
      {:get, :values, :expected_value_name_selection,
       fn -> Flow.get(client, "flow-1", values: [""]) end},
      {:history, :values, :expected_boolean,
       fn -> Flow.history(client, "flow-1", values: "all") end},
      {:complete, :attributes_delete, :expected_name_list,
       fn ->
         Flow.complete(client, "flow-1",
           lease_token: "lease",
           fencing_token: 1,
           attributes_delete: :all
         )
       end},
      {:complete, :attributes_delete, :expected_name_list,
       fn ->
         Flow.complete(client, "flow-1",
           lease_token: "lease",
           fencing_token: 1,
           attributes_delete: ["one" | "two"]
         )
       end},
      {:signal, :drop_values, :expected_name_list,
       fn -> Flow.signal(client, "flow-1", signal: "wake", drop_values: [""]) end}
    ]

    for {operation, option, expectation, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, ^expectation}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow claim collection admission stops at the server footprint bound" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    states = List.duplicate("queued", 100_000)

    {reductions, result} =
      measured_result_reductions(fn ->
        Flow.claim_due(client, "email", worker: "worker", states: states)
      end)

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :claim_due, :states, {:maximum_items, 64}}
            }} = result

    assert reductions < 10_000
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects invalid cross-option invariants before transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    max_exact = 9_007_199_254_740_991

    cases = [
      {:create, :history_hot_max_events, {:must_not_exceed, :history_max_events},
       fn ->
         Flow.create(client, "flow-1",
           type: "email",
           history_hot_max_events: 100,
           history_max_events: 50
         )
       end},
      {:history, :from_ms, {:must_not_exceed, :to_ms},
       fn -> Flow.history(client, "flow-1", from_ms: 2, to_ms: 1) end},
      {:history, :from_version, {:must_not_exceed, :to_version},
       fn -> Flow.history(client, "flow-1", from_version: 2, to_version: 1) end},
      {:history, :from_event, {:must_not_exceed, :to_event},
       fn -> Flow.history(client, "flow-1", from_event: "2-0", to_event: "1-0") end},
      {:claim_due, :lease_ms, {:deadline_exceeds, max_exact},
       fn -> Flow.claim_due(client, "email", worker: "worker", now_ms: max_exact) end},
      {:complete, :ttl_ms, {:deadline_exceeds, max_exact},
       fn ->
         Flow.complete(client, "flow-1",
           lease_token: "lease",
           fencing_token: 1,
           now_ms: max_exact,
           ttl_ms: 1
         )
       end},
      {:value_put, :ttl_ms, {:deadline_exceeds, max_exact},
       fn -> Flow.value_put(client, "value", now_ms: max_exact, ttl_ms: 1) end},
      {:transition, :to_state, :reserved_running_state,
       fn ->
         Flow.transition(client, "flow-1",
           from_state: "queued",
           to_state: "running",
           lease_token: "lease",
           fencing_token: 1
         )
       end},
      {:signal, :transition_to, :reserved_running_state,
       fn -> Flow.signal(client, "flow-1", signal: "wake", transition_to: "running") end},
      {:value_put, :ttl_ms, {:conflicts_with, [:name, :owner_flow_id]},
       fn ->
         Flow.value_put(client, "value", owner_flow_id: "flow-1", name: "result", ttl_ms: 10)
       end}
    ]

    for {operation, option, expectation, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_option, ^operation, ^option, ^expectation}
              }} = call.()
    end

    assert {:error,
            %FerricStore.Error{
              raw: {:conflicting_flow_options, :claim_due, [:partition_key, :partition_keys]}
            }} =
             Flow.claim_due(client, "email",
               worker: "worker",
               partition_key: "p1",
               partition_keys: ["p1"]
             )

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects malformed command identifiers before option work or transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    cases = [
      {:create, :id, nil, fn -> Flow.create(client, nil, type: "email") end},
      {:get, :id, "", fn -> Flow.get(client, "") end},
      {:history, :id, :flow, fn -> Flow.history(client, :flow) end},
      {:claim_due, :type, nil, fn -> Flow.claim_due(client, nil, worker: "worker") end},
      {:policy_get, :type, "", fn -> Flow.policy_get(client, "") end}
    ]

    for {operation, field, value, call} <- cases do
      assert {:error,
              %FerricStore.Error{
                raw:
                  {:invalid_flow_argument, ^operation, ^field, :expected_nonempty_binary, ^value}
              }} = call.()
    end

    max_bytes = FerricStore.RouteKey.max_bytes()
    oversized = :binary.copy("x", max_bytes + 1)

    for {operation, field, call} <- [
          {:create, :id, fn -> Flow.create(client, oversized, type: "email") end},
          {:claim_due, :type, fn -> Flow.claim_due(client, oversized, worker: "worker") end}
        ] do
      assert {:error,
              %FerricStore.Error{
                raw:
                  {:invalid_flow_argument, ^operation, ^field, {:maximum_bytes, ^max_bytes},
                   ^oversized}
              }} = call.()
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "value_mget rejects malformed reference entries before routing or transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    oversized = :binary.copy("r", FerricStore.RouteKey.max_bytes() + 1)

    for refs <- [[""], [123], [oversized], ["ref" | "tail"]] do
      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_value_refs, :expected_nonempty_route_binaries}
              }} = Flow.value_mget(client, refs)
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects invalid collection options instead of changing their meaning" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :claim_due, :states, :expected_state_list}
            }} =
             Flow.claim_due(client, "email", worker: "worker", states: "queued")

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow transport commands preserve the explicit call timeout" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert %{"id" => "flow-1"} = Flow.get(client, "flow-1", call_timeout: 321)
    assert_received {:native, opcode, %{"id" => "flow-1"}, [call_timeout: 321]}
    assert opcode == Protocol.opcode(:flow_get)
  end

  test "Flow collection and enqueue entry points return typed errors for malformed inputs" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_options, :create, :expected_keyword}}} =
             Flow.enqueue(client, "flow-1", :not_options)

    assert {:error, {:invalid_batch_items, :expected_list}} =
             Flow.create_many_payload(:not_items, type: "email")

    assert {:error, %FerricStore.Error{raw: {:invalid_batch_items, :expected_list}}} =
             Flow.create_many(client, :not_items, type: "email")

    assert {:error, {:invalid_batch_items, :expected_list}} =
             Flow.complete_many_payload(:not_jobs)

    assert {:error, %FerricStore.Error{raw: {:invalid_batch_items, :expected_list}}} =
             Flow.complete_many(client, :not_jobs)

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_value_refs, :expected_list}}} =
             Flow.value_mget(client, :not_refs)

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow option admission is bounded before duplicate and keyword scans" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    options = List.duplicate({:type, "email"}, 100_000)

    for call <- [
          fn -> Flow.create(client, "flow-1", options) end,
          fn -> Flow.enqueue(client, "flow-1", options) end
        ] do
      {reductions, result} = measured_result_reductions(call)

      assert {:error,
              %FerricStore.Error{
                raw: {:too_many_flow_options, :create, %{limit: 64, observed: 65}}
              }} = result

      assert reductions < 20_000
    end

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow option maps reject colliding atom and string keys" do
    assert_raise ArgumentError, ~r/duplicate normalized map key.*tenant/, fn ->
      Flow.create_payload("flow-1",
        type: "email",
        attributes: %{:tenant => "atom", "tenant" => "string"}
      )
    end
  end

  test "Flow transport commands return typed errors for colliding map keys" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw:
                {:invalid_flow_option, :create, :attributes,
                 {:duplicate_normalized_map_key, "tenant"}}
            }} =
             Flow.create(client, "flow-1",
               type: "email",
               attributes: %{:tenant => "atom", "tenant" => "string"}
             )

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow establishes the request deadline before recursive option normalization" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create(client, "warmup", type: "email", attributes: %{}, timeout: 0)

    attributes = %{"items" => Enum.to_list(1..100_000)}

    {reductions, result} =
      measured_result_reductions(fn ->
        Flow.create(client, "flow-1", type: "email", attributes: attributes, timeout: 0)
      end)

    assert {:error, %FerricStore.Error{raw: :timeout}} = result
    assert reductions < 20_000
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow stops named-value encoding when the request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    counter = :atomics.new(1, [])

    assert "OK" =
             Flow.create(client, "warmup",
               type: "email",
               values: %{"value" => counter},
               codec: SlowCountingCodec,
               timeout: :infinity
             )

    assert_received {:native, _opcode, _payload, _opts}
    :atomics.put(counter, 1, 0)
    values = Map.new(1..1_000, &{"value-#{&1}", counter})

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create(client, "flow-1",
               type: "email",
               values: values,
               codec: SlowCountingCodec,
               timeout: 2
             )

    assert :atomics.get(counter, 1) < 500
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow rejects oversized named-value maps before codec work" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    counter = :atomics.new(1, [])
    values = Map.new(1..100_001, &{"value-#{&1}", counter})

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :create, :values, :collection_too_large}
            }} =
             Flow.create(client, "flow-1",
               type: "email",
               values: values,
               codec: CountingCodec
             )

    assert :atomics.get(counter, 1) == 0
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow retry returns a typed error for colliding policy keys" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :retry, :retry, {:invalid_policy_option, "retry"}}
            }} =
             Flow.retry(client, "flow-1",
               lease_token: "lease-1",
               fencing_token: 1,
               retry: %{:max_retries => 1, "max_retries" => 2}
             )

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "builds direct create payload with encoded values and string keys" do
    payload =
      Flow.create_payload("flow-1",
        type: "email",
        payload: "hello",
        parent_flow_id: "parent-1",
        root_flow_id: "root-1",
        idempotent: true,
        max_active_ms: 60_000,
        history_hot_max_events: 100,
        history_max_events: 1_000,
        attributes: %{tenant: "acme"},
        state_meta: %{version: 1, owner: "risk"},
        values: %{prompt: "hi"},
        value_refs: %{large: "ref-1"},
        now_ms: 10
      )

    assert payload == %{
             "id" => "flow-1",
             "type" => "email",
             "state" => "queued",
             "now_ms" => 10,
             "run_at_ms" => 10,
             "payload" => "hello",
             "parent_flow_id" => "parent-1",
             "root_flow_id" => "root-1",
             "idempotent" => true,
             "max_active_ms" => 60_000,
             "history_hot_max_events" => 100,
             "history_max_events" => 1_000,
             "attributes" => %{"tenant" => "acme"},
             "state_meta" => %{"version" => 1, "owner" => "risk"},
             "values" => %{"prompt" => "hi"},
             "value_refs" => %{"large" => "ref-1"}
           }
  end

  test "named values preserve application terms for the selected codec" do
    value = %{total: 120, lines: [%{sku: "a-1"}]}

    payload =
      Flow.create_payload("flow-1",
        type: "email",
        values: %{order: value},
        codec: Term,
        now_ms: 10
      )

    assert Term.decode(payload["values"]["order"]) == value
  end

  test "payload normalization rejects state metadata deeper than the wire limit" do
    state_meta = Enum.reduce(1..65, "leaf", fn _level, value -> [value] end)

    assert_raise ArgumentError, ~r/value nesting exceeds 64 levels/, fn ->
      Flow.create_payload("flow-1", type: "email", state_meta: state_meta)
    end
  end

  test "builds direct create many payload" do
    assert Flow.create_many_payload(["flow-1", {"flow-2", "payload"}],
             type: "email",
             now_ms: 10,
             independent: true,
             return_ok_on_success: true
           ) == %{
             "items" => [["flow-1", ""], ["flow-2", "payload"]],
             "type" => "email",
             "state" => "queued",
             "now_ms" => 10,
             "run_at_ms" => 10,
             "independent" => true,
             "return" => "OK_ON_SUCCESS"
           }
  end

  test "empty flow batches are successful no-ops without transport admission" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert Flow.create_many(client, [], []) == []
    assert Flow.complete_many(client, []) == []
    assert Flow.value_mget(client, []) == []

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "create many rejects oversized input before mapping its tail" do
    items = List.duplicate("flow", 100_001) ++ [:invalid_tail]
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             Flow.create_many_payload(items, type: "email")

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000_000
  end

  test "create many performs no codec work for an oversized input" do
    counter = :atomics.new(1, [])
    items = List.duplicate({"flow", counter}, 100_001)

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             Flow.create_many_payload(items, type: "email", codec: CountingCodec)

    assert :atomics.get(counter, 1) == 0
  end

  test "create many stops batch preparation when its established deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    counter = :atomics.new(1, [])

    assert "OK" =
             Flow.create_many(client, [{"warmup", counter}],
               type: "email",
               codec: CountingCodec,
               timeout: :infinity
             )

    assert_received {:native, _opcode, _payload, _opts}
    :atomics.put(counter, 1, 0)

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create_many(client, List.duplicate({"flow", counter}, 100_000),
               type: "email",
               codec: CountingCodec,
               timeout: 1
             )

    assert :atomics.get(counter, 1) < 50_000
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "create many rejects malformed entries without raising or reaching transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    invalid_items = [
      :invalid,
      {123, "payload"},
      %{"id" => "flow-1", "payload" => "body", "paylod" => "typo"},
      %{:id => "flow-1", "id" => "duplicate"}
    ]

    Enum.each(invalid_items, fn item ->
      assert {:error, {:invalid_flow_create_many_item, ^item}} =
               Flow.create_many_payload(["valid", item], type: "email")

      assert {:error, %FerricStore.Error{raw: {:invalid_flow_create_many_item, ^item}}} =
               Flow.create_many(client, ["valid", item], type: "email")
    end)

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "keeps compact create-many payload as iodata until request encoding" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert "OK" =
             Flow.create_many(client, ["flow-1", {"flow-2", "payload"}],
               type: "email",
               now_ms: 10,
               timeout: :infinity
             )

    assert_received {:native, opcode, {:custom_payload, body}, [timeout: :infinity]}

    assert opcode == Protocol.opcode(:flow_create_many)
    assert is_list(body)
    assert IO.iodata_length(body) > 0
  end

  test "infinite-timeout batches keep partition routing instead of using the control connection" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    partition = "tenant:a"
    digest = partition |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)
    route = {:slot, Bitwise.band(:erlang.crc32("f:#{digest}"), 1_023)}

    assert "OK" =
             Flow.create_many(client, ["flow-1"],
               type: "email",
               partition_key: partition,
               timeout: :infinity
             )

    assert_received {:native_route, ^route}
    assert_received {:native, opcode, %{"partition_key" => ^partition}, [timeout: :infinity]}
    assert opcode == Protocol.opcode(:flow_create_many)
  end

  test "compact create-many reuses its admitted item count at the batch limit" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    items = List.duplicate("flow", 100_000)

    assert "OK" =
             Flow.create_many(client, ["warmup"],
               type: "email",
               now_ms: 10,
               timeout: :infinity
             )

    assert_received {:native, _opcode, _payload, _opts}

    reductions =
      measured_reductions(fn ->
        assert "OK" =
                 Flow.create_many(client, items,
                   type: "email",
                   now_ms: 10,
                   timeout: :infinity
                 )
      end)

    assert reductions < 1_500_000
    assert_received {:native, _opcode, {:custom_payload, _body}, _opts}
  end

  test "finite create-many requests stay typed so the session can add a deadline" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert "OK" =
             Flow.create_many(client, ["flow-1"], type: "email", now_ms: 10, timeout: 250)

    assert_received {:native, opcode, %{"items" => [["flow-1", ""]]} = payload, [timeout: 250]}

    assert opcode == Protocol.opcode(:flow_create_many)
    refute match?({:custom_payload, _body}, payload)
  end

  test "create-many preserves idempotency in the command payload and retry policy" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert "OK" =
             Flow.create_many(client, ["flow-1"],
               type: "email",
               idempotent: true,
               now_ms: 10
             )

    assert_received {:native, opcode, %{"idempotent" => true, "items" => [["flow-1", ""]]},
                     [idempotent: true]}

    assert opcode == Protocol.opcode(:flow_create_many)
  end

  test "builds direct claim due payload" do
    payload =
      Flow.claim_due_payload("email",
        state: "queued",
        worker: "w1",
        now_ms: 100,
        limit: 10,
        include_attributes: false,
        partition_keys: ["p1", "p2"]
      )

    assert payload == %{
             "type" => "email",
             "state" => "queued",
             "worker" => "w1",
             "lease_ms" => 30_000,
             "limit" => 10,
             "now_ms" => 100,
             "return" => "JOBS_COMPACT",
             "partition_keys" => ["p1", "p2"]
           }
  end

  test "claim rejects conflicting singular and plural state filters" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw: {:conflicting_flow_options, :claim_due, [:state, :states]}
            }} =
             Flow.claim_due(client, "email",
               worker: "worker",
               state: "queued",
               states: ["running"]
             )

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "large repeated flow filters are built in linear time" do
    Flow.claim_due_payload("email", worker: "warmup", states: ["queued"])

    small = measured_reductions(fn -> claim_payload_with_states(1_000) end)
    large = measured_reductions(fn -> claim_payload_with_states(2_000) end)

    assert large < small * 3
  end

  test "claim due normalizes compact jobs with partition, lease, and fencing fields" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert [
             %{
               "id" => "flow-1",
               "partition_key" => "tenant:a",
               "lease_token" => "lease-1",
               "fencing_token" => 10
             },
             %{
               "id" => "flow-2",
               "partition_key" => "tenant:b",
               "lease_token" => "lease-2",
               "fencing_token" => 11,
               "attributes" => %{"tenant" => "acme"}
             }
           ] =
             Flow.claim_due(client, "email",
               state: "queued",
               worker: "w1",
               partition_keys: ["tenant:a", "tenant:b"],
               include_attributes: false
             )

    assert_received {:native, opcode,
                     %{
                       "type" => "email",
                       "state" => "queued",
                       "worker" => "w1",
                       "partition_keys" => ["tenant:a", "tenant:b"]
                     }, []}

    assert opcode == Protocol.opcode(:flow_claim_due)
  end

  test "claim due exposes compact claimed state as run_state" do
    {:ok, state_only} =
      ClaimShapeClient.start_link([
        ["flow-state", "tenant:a", "lease-state", 12, "queued"]
      ])

    assert [
             %{
               "id" => "flow-state",
               "partition_key" => "tenant:a",
               "lease_token" => "lease-state",
               "fencing_token" => 12,
               "run_state" => "queued"
             }
           ] =
             Flow.claim_due(state_only, "email",
               state: "queued",
               worker: "w1",
               include_state: true,
               include_attributes: false
             )

    {:ok, state_and_attributes} =
      ClaimShapeClient.start_link([
        ["flow-attrs", "tenant:b", "lease-attrs", 13, "ready", %{"tenant" => "acme"}]
      ])

    assert [
             %{
               "id" => "flow-attrs",
               "partition_key" => "tenant:b",
               "lease_token" => "lease-attrs",
               "fencing_token" => 13,
               "run_state" => "ready",
               "attributes" => %{"tenant" => "acme"}
             }
           ] =
             Flow.claim_due(state_and_attributes, "email",
               state: "ready",
               worker: "w1",
               include_state: true
             )
  end

  defp claim_payload_with_states(count) do
    states = Enum.map(1..count, &"state-#{&1}")
    payload = Flow.claim_due_payload("email", worker: "worker", states: states)
    assert length(payload["states"]) == count
  end

  defp measured_reductions(fun) do
    {:reductions, before_count} = Process.info(self(), :reductions)
    fun.()
    {:reductions, after_count} = Process.info(self(), :reductions)
    after_count - before_count
  end

  defp measured_result_reductions(fun) do
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)
    result = fun.()
    {:reductions, after_count} = Process.info(self(), :reductions)
    {after_count - before_count, result}
  end

  test "builds direct transition payload with partition and fencing data" do
    assert Flow.transition_payload("flow-1",
             from_state: "running",
             to_state: "ready",
             partition_key: "tenant:a",
             lease_token: "lease-1",
             fencing_token: 7,
             payload: "next",
             state_meta: %{version: 2},
             now_ms: 100,
             run_at_ms: 200
           ) == %{
             "id" => "flow-1",
             "from_state" => "running",
             "to_state" => "ready",
             "partition_key" => "tenant:a",
             "lease_token" => "lease-1",
             "fencing_token" => 7,
             "payload" => "next",
             "state_meta" => %{"version" => 2},
             "now_ms" => 100,
             "run_at_ms" => 200
           }
  end

  test "transition sends typed native payload" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert Flow.transition(client, "flow-1",
             from_state: "running",
             to_state: "ready",
             partition_key: "tenant:a",
             lease_token: "lease-1",
             fencing_token: 7,
             now_ms: 100
           ) == "OK"

    assert_received {:native, opcode,
                     %{
                       "id" => "flow-1",
                       "from_state" => "running",
                       "to_state" => "ready",
                       "partition_key" => "tenant:a",
                       "lease_token" => "lease-1",
                       "fencing_token" => 7
                     }, []}

    assert opcode == Protocol.opcode(:flow_transition)
  end

  test "builds direct complete payload" do
    assert Flow.complete_payload("flow-1",
             lease_token: "token",
             fencing_token: 5,
             result: "ok",
             state_meta: %{version: 3},
             now_ms: 10
           ) == %{
             "id" => "flow-1",
             "lease_token" => "token",
             "fencing_token" => 5,
             "result" => "ok",
             "now_ms" => 10,
             "state_meta" => %{"version" => 3}
           }
  end

  test "complete payloads require the fencing token used by the current server" do
    assert_raise KeyError, fn ->
      Flow.complete_payload("flow-1", lease_token: "token", result: "ok")
    end
  end

  test "terminal payloads never alias result and error fields" do
    complete =
      Flow.complete_payload("flow-1",
        lease_token: "token",
        fencing_token: 1,
        error: "not-a-result"
      )

    fail =
      Flow.fail_payload("flow-1",
        lease_token: "token",
        fencing_token: 1,
        result: "not-an-error"
      )

    refute Map.has_key?(complete, "result")
    refute Map.has_key?(complete, "error")
    refute Map.has_key?(fail, "result")
    refute Map.has_key?(fail, "error")
  end

  test "builds flow policy and search payloads for indexed state metadata" do
    assert Flow.policy_set_payload("review", indexed_state_meta: "version") == %{
             "type" => "review",
             "indexed_state_meta" => "version"
           }

    assert Flow.policy_set_payload("review", indexed_state_meta: nil) == %{
             "type" => "review",
             "indexed_state_meta" => nil
           }

    assert Flow.search_payload(
             type: "review",
             state: "accept",
             partition_key: "tenant-1",
             state_meta: %{version: 1},
             consistent_projection: true,
             terminal_only: true,
             count: 10
           ) == %{
             "type" => "review",
             "state" => "accept",
             "partition_key" => "tenant-1",
             "state_meta" => %{"accept" => %{"version" => 1}},
             "consistent_projection" => true,
             "terminal_only" => true,
             "count" => 10
           }
  end

  test "builds state policy payloads for FIFO and parallel modes" do
    assert Flow.policy_set_payload("review",
             indexed_attributes: ["tenant", "region"],
             retry: [max_retries: 5, backoff: [kind: :exponential, base_ms: 100]],
             retention: [ttl_ms: 60_000],
             states: %{
               "queued" => [mode: :fifo],
               "reviewing" => %{mode: :parallel}
             }
           ) == %{
             "type" => "review",
             "indexed_attributes" => ["tenant", "region"],
             "retry" => %{
               "max_retries" => 5,
               "backoff" => %{"kind" => :exponential, "base_ms" => 100}
             },
             "retention" => %{"ttl_ms" => 60_000},
             "states" => %{
               "queued" => %{"mode" => :fifo},
               "reviewing" => %{"mode" => :parallel}
             }
           }

    assert Flow.policy_set_payload("review", []) == %{"type" => "review"}

    assert Flow.policy_get_payload("review", state: "queued") == %{
             "type" => "review",
             "state" => "queued"
           }
  end

  test "policy payloads preserve empty index-name arrays" do
    assert Flow.policy_set_payload("review",
             indexed_attributes: [],
             indexed_state_meta: []
           ) == %{
             "type" => "review",
             "indexed_attributes" => [],
             "indexed_state_meta" => []
           }

    {:ok, client} = CaptureNativeClient.start_link(self())

    assert %PolicySnapshot{
             generation: 1,
             indexed_attributes: [],
             indexed_state_meta: []
           } =
             Flow.policy_set(client, "review",
               indexed_attributes: [],
               indexed_state_meta: []
             )

    assert_received {:native, opcode,
                     %{
                       "type" => "review",
                       "indexed_attributes" => [],
                       "indexed_state_meta" => []
                     }, []}

    assert opcode == Protocol.opcode(:flow_policy_set)
  end

  test "policy helpers use the canonical typed native contract" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert %PolicySnapshot{
             type: "review",
             generation: 1,
             indexed_state_meta: "version",
             states: %{"queued" => %{"mode" => :fifo}}
           } =
             Flow.policy_set(client, "review",
               indexed_state_meta: "version",
               states: %{"queued" => [mode: :fifo]}
             )

    assert_received {:native, set_opcode,
                     %{
                       "type" => "review",
                       "indexed_state_meta" => "version",
                       "states" => %{"queued" => %{"mode" => :fifo}}
                     }, []}

    assert set_opcode == Protocol.opcode(:flow_policy_set)

    assert %PolicySnapshot{type: "review", generation: 1, state: "queued"} =
             Flow.policy_get(client, "review", state: "queued")

    assert_received {:native, get_opcode, %{"type" => "review", "state" => "queued"}, []}
    assert get_opcode == Protocol.opcode(:flow_policy_get)
    refute_received {:command, _command, _args, _opts}
  end

  test "policy payload preserves retry, retention, index, and state options" do
    assert Flow.policy_set_payload("review",
             indexed_state_meta: "version",
             indexed_attributes: ["tenant", "region"],
             max_active_ms: 60_000,
             retry: [
               max_retries: 5,
               backoff: [kind: :exponential, base_ms: 100, max_ms: 1_000, jitter_pct: 10],
               exhausted_to: "failed"
             ],
             retention: [ttl_ms: 60_000, history_max_events: 1_000],
             states: [
               {"queued", [mode: :fifo, retry: [max_retries: 1]]}
             ]
           ) == %{
             "type" => "review",
             "indexed_state_meta" => "version",
             "indexed_attributes" => ["tenant", "region"],
             "max_active_ms" => 60_000,
             "retry" => %{
               "max_retries" => 5,
               "backoff" => %{
                 "kind" => :exponential,
                 "base_ms" => 100,
                 "max_ms" => 1_000,
                 "jitter_pct" => 10
               },
               "exhausted_to" => "failed"
             },
             "retention" => %{"ttl_ms" => 60_000, "history_max_events" => 1_000},
             "states" => %{
               "queued" => %{"mode" => :fifo, "retry" => %{"max_retries" => 1}}
             }
           }
  end

  test "unsupported policy fields fail before a request can silently lose them" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:unsupported_policy_options, ["governance"]}}} =
             Flow.policy_set(client, "review", governance: %{approval: true})

    refute_received {:native, _opcode, _payload, _opts}
    refute_received {:command, _command, _args, _opts}
  end

  test "malformed policy states fail before a request can silently lose them" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:invalid_policy_option, "states"}}} =
             Flow.policy_set(client, "review", states: ["queued"])

    refute_received {:native, _opcode, _payload, _opts}
    refute_received {:command, _command, _args, _opts}
  end

  test "duplicate policy states fail instead of silently taking the last policy" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: {:duplicate_policy_states, ["queued"]}}} =
             Flow.policy_set(client, "review",
               states: [{"queued", [mode: :fifo]}, {:queued, [mode: :parallel]}]
             )

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "nested policy option admission is bounded" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    retry = List.duplicate({:max_retries, 1}, 100_000)

    {reductions, result} =
      measured_result_reductions(fn -> Flow.policy_set(client, "review", retry: retry) end)

    assert {:error, %FerricStore.Error{raw: {:invalid_policy_option, "retry"}}} = result
    assert reductions < 20_000
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "large policy state sets are built in linear time" do
    policy_payload_with_states(10)

    small = measured_reductions(fn -> policy_payload_with_states(1_000) end)
    large = measured_reductions(fn -> policy_payload_with_states(4_000) end)

    assert large < small * 6,
           "expected linear policy construction, got #{small} reductions for 1000 states and #{large} for 4000"
  end

  test "policy state normalization stops when the established request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())
    states = Enum.map(1..100_000, &{"state-#{&1}", [mode: :fifo]})

    {reductions, result} =
      measured_result_reductions(fn ->
        Flow.policy_set(client, "review", states: states, timeout: 10)
      end)

    assert {:error, %FerricStore.Error{raw: :timeout}} = result
    assert reductions < 5_000_000
    refute_received {:native, _opcode, _payload, _opts}
  end

  defp policy_payload_with_states(count) do
    states = Enum.map(1..count, &{"state-#{&1}", [mode: :fifo]})
    payload = Flow.policy_set_payload("review", states: states)
    assert map_size(payload["states"]) == count
  end

  test "builds direct complete many payload" do
    jobs = [
      %{"id" => "flow-1", "partition_key" => "p1", "lease_token" => "l1", "fencing_token" => 1},
      %{"id" => "flow-2", "lease_token" => "l2", "fencing_token" => 2}
    ]

    assert Flow.complete_many_payload(jobs, now_ms: 10, return_ok_on_success: true) == %{
             "items" => [["flow-1", "p1", "l1", 1], ["flow-2", "l2", 2]],
             "now_ms" => 10,
             "return" => "OK_ON_SUCCESS"
           }
  end

  test "complete many rejects oversized input before mapping its tail" do
    job = {"flow", "lease", 1}
    jobs = List.duplicate(job, 100_001) ++ [:invalid_tail]

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             Flow.complete_many_payload(jobs)
  end

  test "complete many rejects malformed entries without raising or reaching transport" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    invalid_jobs = [
      :invalid,
      {"flow", "lease", -1},
      %{"id" => "flow", "lease_token" => "lease"},
      %{
        "id" => "flow",
        "lease_token" => "lease",
        "fencing_token" => 1,
        "fencing_tokn" => 1
      }
    ]

    Enum.each(invalid_jobs, fn job ->
      assert {:error, {:invalid_flow_complete_many_item, ^job}} =
               Flow.complete_many_payload([job])

      assert {:error, %FerricStore.Error{raw: {:invalid_flow_complete_many_item, ^job}}} =
               Flow.complete_many(client, [job])
    end)

    refute_received {:native, _opcode, _payload, _opts}
  end

  test "finite complete-many requests stay typed so the session can add a deadline" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert "OK" =
             Flow.complete_many(client, [{"flow", "lease", 1}], now_ms: 10, timeout: 250)

    assert_received {:native, opcode, %{"items" => [["flow", "lease", 1]]} = payload,
                     [timeout: 250]}

    assert opcode == Protocol.opcode(:flow_complete_many)
    refute match?({:custom_payload, _body}, payload)
  end

  test "get and value_mget decode hydrated values with the selected codec" do
    codec = FerricStore.Codec.Term

    record = %{
      "id" => "flow-1",
      "payload" => codec.encode(%{step: 1}),
      "result" => codec.encode(false),
      "values" => %{"invoice" => codec.encode(%{total: 120})}
    }

    values = [codec.encode(%{large: true}), nil]
    {:ok, client} = CaptureReadClient.start_link(self(), record, values)

    assert %{
             "payload" => %{step: 1},
             "result" => false,
             "values" => %{"invoice" => %{total: 120}}
           } = Flow.get(client, "flow-1", codec: codec, timeout: 321)

    assert_received {:native, opcode, %{"id" => "flow-1"}, [timeout: 321]}
    assert opcode == Protocol.opcode(:flow_get)

    assert [%{large: true}, nil] =
             Flow.value_mget(client, ["ref-1", "ref-2"], codec: codec, timeout: 654)

    assert_received {:native, opcode, %{"refs" => ["ref-1", "ref-2"]}, [timeout: 654]}
    assert opcode == Protocol.opcode(:flow_value_mget)
  end

  test "Flow stops application response decoding when the request deadline expires" do
    encoded = SlowDecodeCodec.encode({self(), 1_000, :decoded})
    {:ok, client} = CaptureReadClient.start_link(self(), %{"payload" => encoded}, [])

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.get(client, "flow-1", codec: SlowDecodeCodec, timeout: 250)

    assert_receive {:slow_flow_decoder, decoder}
    refute decoder == self()
    monitor = Process.monitor(decoder)
    assert_receive {:DOWN, ^monitor, :process, ^decoder, _reason}, 1_000
  end

  test "Flow stops scalar application encoding when the request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create(client, "flow-1",
               type: "email",
               payload: {self(), 250, "encoded"},
               codec: SlowEncodeCodec,
               timeout: 50
             )

    assert_stopped_encoder()
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow stops named-value application encoding when the request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create(client, "flow-1",
               type: "email",
               values: %{invoice: {self(), 250, "encoded"}},
               codec: SlowEncodeCodec,
               timeout: 50
             )

    assert_stopped_encoder()
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow stops value_put application encoding when the request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.value_put(client, {self(), 250, "encoded"},
               codec: SlowEncodeCodec,
               timeout: 50
             )

    assert_stopped_encoder()
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow stops batched application encoding when the request deadline expires" do
    {:ok, client} = CaptureNativeClient.start_link(self())

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.create_many(client, [{"flow-1", {self(), 250, "encoded"}}],
               type: "email",
               codec: SlowEncodeCodec,
               timeout: 50
             )

    assert_stopped_encoder()
    refute_received {:native, _opcode, _payload, _opts}
  end

  test "Flow stops large claim response normalization at the request deadline" do
    jobs = List.duplicate(["flow", "partition", "lease", 1], 100_000)
    {:ok, client} = ClaimShapeClient.start_link(jobs)
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, %FerricStore.Error{raw: :timeout}} =
             Flow.claim_due(client, "email", worker: "worker", timeout: 1)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 1_000_000
  end

  test "claim_due rejects malformed lease credentials and response shapes" do
    cases = [
      {"OK", :expected_list},
      {[["flow-1", "lease", -1]], :invalid_claim},
      {[["flow-1", "partition", "lease", 1, []]], :invalid_claim},
      {[%{"id" => "flow-1", "fencing_token" => 1}], :invalid_claim}
    ]

    for {response, reason} <- cases do
      {:ok, client} = ClaimShapeClient.start_link(response)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: :claim_due, reason: ^reason}}
              }} = Flow.claim_due(client, "email", worker: "worker")
    end
  end

  test "claim_due rejects improper response lists for raw and application codecs" do
    response = [["flow-1", nil, "lease", 1] | :invalid_tail]

    for opts <- [[], [codec: Term]] do
      {:ok, client} = ClaimShapeClient.start_link(response)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: :claim_due, reason: :expected_list}}
              }} = Flow.claim_due(client, "email", [worker: "worker"] ++ opts)
    end
  end

  test "Flow reads reject malformed record response shapes" do
    cases = [
      {:get, "OK", :expected_record_or_nil, fn client -> Flow.get(client, "flow-1") end},
      {:list, %{}, :expected_record_list, fn client -> Flow.list(client, type: "email") end},
      {:history, [["1-0", %{}], "bad"], :invalid_history_entry,
       fn client -> Flow.history(client, "flow-1") end},
      {:search, nil, :expected_record_list, fn client -> Flow.search(client, type: "email") end}
    ]

    for {operation, response, reason, call} <- cases do
      {:ok, client} = ClaimShapeClient.start_link(response)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: ^operation, reason: ^reason}}
              }} = call.(client)
    end
  end

  test "value_mget rejects truncated, oversized, and invalid response items" do
    cases = [
      {["one"], :unexpected_cardinality},
      {["one", nil, "three"], :unexpected_cardinality},
      {["one", 2], :expected_binary_or_nil}
    ]

    Enum.each(cases, fn {values, reason} ->
      {:ok, client} = CaptureReadClient.start_link(self(), %{}, values)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: :value_mget, reason: ^reason}}
              }} = Flow.value_mget(client, ["ref-1", "ref-2"])
    end)
  end

  test "value_mget rejects improper response lists for raw and application codecs" do
    cases = [
      {["one" | :invalid_tail], []},
      {[Term.encode(:one) | :invalid_tail], [codec: Term]}
    ]

    for {values, opts} <- cases do
      {:ok, client} = CaptureReadClient.start_link(self(), %{}, values)

      assert {:error,
              %FerricStore.Error{
                raw: {:invalid_flow_response, %{operation: :value_mget, reason: :expected_list}}
              }} = Flow.value_mget(client, ["ref-1", "ref-2"], opts)
    end
  end

  test "malformed codec values become typed errors instead of crashing callers" do
    codec = FerricStore.Codec.Term
    invalid_record = %{"id" => "flow-1", "payload" => <<131, 80, 0, 0, 0, 1>>}
    {:ok, client} = CaptureReadClient.start_link(self(), invalid_record, [<<131, 1>>])

    assert {:error, %FerricStore.Error{raw: {:flow_codec_decode_failed, ^codec}}} =
             Flow.get(client, "flow-1", codec: codec)

    assert {:error, %FerricStore.Error{raw: {:flow_codec_decode_failed, ^codec}}} =
             Flow.value_mget(client, ["ref-1"], codec: codec)
  end

  defp assert_stopped_encoder do
    assert_receive {:slow_flow_encoder, encoder}
    refute encoder == self()
    monitor = Process.monitor(encoder)
    assert_receive {:DOWN, ^monitor, :process, ^encoder, _reason}, 1_000
  end
end
