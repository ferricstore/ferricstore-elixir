defmodule FerricStore.SDK.FlowTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestLimits
  alias FerricStore.SDK.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
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

    def handle_call({:request, opcode, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request, opcode, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, opcode, key, payload, opts}, _from, test_pid) do
      opts = FerricStore.RequestContext.options(opts)
      send(test_pid, {:request_by_key, opcode, key, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end
  end

  defmodule LimitedClient do
    use GenServer

    def start_link(test_pid, limit),
      do:
        GenServer.start_link(__MODULE__, {test_pid, limit})
        |> ClientRuntime.wrap()

    @impl true
    def init({test_pid, limit}), do: {:ok, %{limit: limit, test_pid: test_pid}}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({:request, opcode, payload, opts}, _from, state) do
      reply(opcode, payload, opts, state)
    end

    def handle_call({:command, opcode, _key, payload, opts}, _from, state) do
      reply(opcode, payload, opts, state)
    end

    defp reply(opcode, payload, context, state) do
      case RequestLimits.admit(context.batch_item_count, state.limit) do
        :ok ->
          send(state.test_pid, {:admitted, opcode})
          {:reply, {:ok, payload}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    {:ok, client: client}
  end

  test "latest flow search wrapper routes by partition key and keeps terminal filter", %{
    client: client
  } do
    assert {:ok, %{"type" => "review", "partition_key" => "tenant:a", "terminal_only" => true}} =
             Flow.search(client, %{type: "review", partition_key: "tenant:a", terminal_only: true})

    assert_received {:request_by_key, opcode, route,
                     %{
                       "type" => "review",
                       "partition_key" => "tenant:a",
                       "terminal_only" => true
                     }, []}

    assert opcode == Opcodes.flow_search()
    assert route == partition_route_key("tenant:a")
  end

  test "latest schedule wrapper uses typed schedule opcode", %{client: client} do
    assert {:ok, %{"type" => "daily", "cron" => "* * * * *"}} =
             Flow.schedule_create(client, %{type: "daily", cron: "* * * * *"})

    assert_received {:request, opcode, %{"type" => "daily", "cron" => "* * * * *"}, []}
    assert opcode == Opcodes.flow_schedule_create()
  end

  test "latest governance wrapper routes by scope", %{client: client} do
    assert {:ok, %{"scope" => "tenant:a:budget", "amount" => 10}} =
             Flow.budget_release(client, %{scope: "tenant:a:budget", amount: 10})

    assert_received {:request_by_key, opcode, route,
                     %{"scope" => "tenant:a:budget", "amount" => 10}, []}

    assert opcode == Opcodes.flow_budget_release()
    assert route == partition_route_key("tenant:a:budget")
  end

  test "policy wrappers use typed native execution on the control path", %{client: client} do
    assert {:ok,
            %{
              "type" => "review",
              "indexed_state_meta" => "version",
              "states" => %{"queued" => %{"mode" => :fifo}}
            }} =
             Flow.policy_set(client, %{
               type: "review",
               indexed_state_meta: "version",
               states: %{"queued" => %{mode: :fifo}}
             })

    assert_received {:request, opcode,
                     %{
                       "type" => "review",
                       "indexed_state_meta" => "version",
                       "states" => %{"queued" => %{"mode" => :fifo}}
                     }, []}

    assert opcode == Opcodes.flow_policy_set()
  end

  test "policy payload transport fields are rejected instead of silently discarded", %{
    client: client
  } do
    assert {:error, {:unsupported_policy_options, ["timeout"]}} =
             Flow.policy_set(client, %{type: "review", timeout: 10})

    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "policy wrappers reject oversized nested options before recursive normalization", %{
    client: client
  } do
    retry = List.duplicate({:max_retries, 1}, 100_000)
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, {:invalid_policy_option, "retry"}} =
             Flow.policy_set(client, %{type: "review", retry: retry})

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
    refute_received {:request, _opcode, _payload, _opts}
  end

  test "policy deadlines are established before policy payload normalization", %{client: client} do
    assert {:error, :timeout} =
             Flow.policy_set(client, %{type: "warmup"}, timeout: 0, call_timeout: 0)

    payload = Map.put(Map.new(1..100_000, &{"field-#{&1}", &1}), :type, "review")
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} =
             Flow.policy_set(client, payload, timeout: 0, call_timeout: 0)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "policy state validation stops when the established request deadline expires", %{
    client: client
  } do
    states = Map.new(1..100_000, &{"state-#{&1}", %{mode: :fifo}})
    payload = %{type: "review", states: states}
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} = Flow.policy_set(client, payload, timeout: 10)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 5_000_000
    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "payload normalization rejects colliding atom and string keys", %{client: client} do
    assert {:error, {:duplicate_normalized_map_key, "id"}} =
             Flow.get(client, %{:id => "atom", "id" => "string"})

    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "flow deadlines are established before typed payload normalization", %{client: client} do
    assert {:error, :timeout} = Flow.stats(client, %{}, timeout: 0, call_timeout: 0)

    payload = Map.new(1..100_000, &{"field-#{&1}", &1})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} = Flow.stats(client, payload, timeout: 0, call_timeout: 0)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "flow routing observes the preprocessing deadline while scanning route lists", %{
    client: client
  } do
    assert {:error, :timeout} = Flow.value_mget(client, %{refs: []}, timeout: 0)

    payload = %{refs: List.duplicate("f:{same}:value", 100_000)}
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} = Flow.value_mget(client, payload, timeout: 5)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 900_000
    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "flow helpers reject non-map payload containers without raising", %{client: client} do
    for call <- [
          fn -> Flow.get(client, :not_a_map) end,
          fn -> Flow.request(client, :flow_get, :not_a_map) end,
          fn -> Flow.policy_set(client, :not_a_map) end
        ] do
      assert {:error, {:invalid_flow_payload, %{reason: :expected_map, value: :not_a_map}}} =
               call.()
    end

    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "oversized typed batches are rejected before recursive payload normalization", %{
    client: client
  } do
    items =
      List.duplicate(
        %{id: "flow-id", nested: %{metadata: %{attempt: 1}}},
        FerricStore.RequestLimits.max_batch_items() + 1
      )

    {:reductions, before} = Process.info(self(), :reductions)

    assert {:error, {:batch_too_large, %{items: 100_001, limit: 100_000}}} =
             Flow.create_many(client, %{items: items})

    {:reductions, after_request} = Process.info(self(), :reductions)

    assert after_request - before < 500_000
    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "configured batch limits reject before recursive payload normalization" do
    {:ok, client} = LimitedClient.start_link(self(), 2)

    items =
      List.duplicate(
        %{id: "flow-id", nested: %{metadata: %{attempt: 1}}},
        RequestLimits.max_batch_items()
      )

    {:reductions, before} = Process.info(self(), :reductions)

    assert {:error, {:batch_too_large, %{items: 3, limit: 2}}} =
             Flow.create_many(client, %{items: items})

    {:reductions, after_request} = Process.info(self(), :reductions)

    assert after_request - before < 500_000
    refute_received {:admitted, _opcode}
  end

  test "typed Flow batches traverse cardinality once before coordinator admission" do
    {:ok, client} = LimitedClient.start_link(self(), RequestLimits.max_batch_items())
    items = List.duplicate(%{id: "flow-id"}, RequestLimits.max_batch_items())

    {:reductions, before} = Process.info(self(), :reductions)
    assert {:ok, %{"items" => ^items}} = Flow.create_many(client, %{items: items})
    {:reductions, after_request} = Process.info(self(), :reductions)

    assert after_request - before < 180_000
    assert_received {:admitted, _opcode}
  end

  test "non-binary route fields return a validation error instead of crashing", %{client: client} do
    assert {:error, {:invalid_route_key, 123}} = Flow.get(client, %{id: 123})

    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "an explicit invalid route key is not replaced by a payload route", %{client: client} do
    assert {:error, {:invalid_route_key, 123}} =
             Flow.get(client, %{id: "valid-payload-id"}, route_key: 123)

    refute_received {:request, _opcode, _payload, _opts}
    refute_received {:request_by_key, _opcode, _key, _payload, _opts}
  end

  test "state-id payloads use the server's auto-partition route", %{client: client} do
    assert {:ok, %{"id" => "flow-id"}} = Flow.get(client, %{id: "flow-id"})

    assert_received {:request_by_key, opcode, route, %{"id" => "flow-id"}, []}
    assert opcode == Opcodes.flow_get()
    assert route == auto_route_key("flow-id")
  end

  defp auto_route_key(id) do
    bucket = rem(:erlang.crc32(id), 256)
    "f:{fa:#{bucket}}:route"
  end

  defp partition_route_key(partition) do
    digest = partition |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)
    "f:{f:#{digest}}:route"
  end
end
