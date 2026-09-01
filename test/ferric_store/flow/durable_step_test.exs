defmodule FerricStore.Flow.DurableStepTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Flow
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.Test.ClientRuntime

  @step_name "charge-customer:v1"
  @step_key "__ferricstore_step__:sha256:ea8eb3a35639b63a2fd520c0ec03b3c5508553f55f02f6e52e8ac5d9e37121b7"

  defmodule ScriptedClient do
    use GenServer

    alias FerricStore.RequestContext

    def start_link(owner, replies) do
      GenServer.start_link(__MODULE__, {owner, replies})
      |> ClientRuntime.wrap()
    end

    @impl true
    def init({owner, replies}), do: {:ok, %{owner: owner, replies: replies}}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({:request, opcode, payload, context}, _from, state),
      do: reply(opcode, nil, payload, context, state)

    def handle_call({:command, opcode, route, payload, context}, _from, state),
      do: reply(opcode, route, payload, context, state)

    defp reply(opcode, route, payload, context, %{owner: owner, replies: [reply | rest]} = state) do
      send(owner, {:request, opcode, route, payload, RequestContext.options(context)})
      {:reply, reply, %{state | replies: rest}}
    end
  end

  defmodule NormalizingCodec do
    @behaviour FerricStore.Codec

    @impl true
    def encode(value), do: :erlang.term_to_binary(value)

    @impl true
    def decode(value), do: {:decoded, :erlang.binary_to_term(value, [:safe])}
  end

  test "advance infers claim credentials and returns the refreshed claim" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, ["flow-1", "tenant-a", "lease-2", 8]}
      ])

    job = claimed_job()

    assert %{
             "id" => "flow-1",
             "partition_key" => "tenant-a",
             "lease_token" => "lease-2",
             "fencing_token" => 8,
             "run_state" => "schedule_warning"
           } = Flow.advance(client, job, to_state: "schedule_warning", now_ms: 1_000)

    assert_receive {:request, opcode, _route,
                    %{
                      "id" => "flow-1",
                      "partition_key" => "tenant-a",
                      "lease_token" => "lease-1",
                      "fencing_token" => 7,
                      "from_state" => "charge",
                      "to_state" => "schedule_warning",
                      "now_ms" => 1_000,
                      "return" => "JOBS_COMPACT"
                    }, []}

    assert opcode == Opcodes.flow_step_continue()

    {:ok, full_client} = ScriptedClient.start_link(self(), [{:ok, refreshed_map()}])

    assert %{"run_state" => "schedule_warning", "lease_token" => "lease-2"} =
             Flow.advance(full_client, job, to_state: "schedule_warning")

    assert_receive {:request, ^opcode, _route, _payload, []}
  end

  test "step validates, executes once, and commits the encoded result with the transition" do
    result = %{charge_id: "ch_123", amount: 150}

    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:ok, ["flow-1", "tenant-a", "lease-2", 8]}
      ])

    assert {refreshed, ^result} =
             Flow.step(client, Map.put(claimed_job(), "state", "running"),
               name: @step_name,
               run: fn -> result end,
               to_state: "schedule_warning",
               codec: Term,
               lease_ms: 60_000,
               now_ms: 1_000
             )

    assert refreshed["lease_token"] == "lease-2"
    assert refreshed["fencing_token"] == 8
    assert refreshed["run_state"] == "schedule_warning"

    assert_receive {:request, extend_opcode, _route,
                    %{
                      "id" => "flow-1",
                      "partition_key" => "tenant-a",
                      "lease_token" => "lease-1",
                      "fencing_token" => 7,
                      "lease_ms" => 60_000,
                      "now_ms" => 1_000
                    }, []}

    assert extend_opcode == Opcodes.flow_extend_lease()

    assert_receive {:request, continue_opcode, _route, payload, []}
    assert continue_opcode == Opcodes.flow_step_continue()
    assert payload["values"] == %{@step_key => Term.encode(result)}
    assert payload["return"] == "JOBS_COMPACT"
    refute Map.has_key?(payload, "worker")
  end

  test "step round-trips a binary result with the default Raw codec" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:ok, ["flow-1", "tenant-a", "lease-2", 8]}
      ])

    assert {refreshed, "charged"} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: fn -> "charged" end,
               to_state: "schedule_warning"
             )

    assert refreshed["run_state"] == "schedule_warning"
    assert_receive {:request, _extend_opcode, _route, _payload, []}
    assert_receive {:request, continue_opcode, _route, payload, []}
    assert continue_opcode == Opcodes.flow_step_continue()
    assert payload["values"] == %{@step_key => "charged"}
  end

  test "newly committed and replayed results use identical codec-decoded semantics" do
    encoded = NormalizingCodec.encode(:charged)

    {:ok, first_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:ok, ["flow-1", "tenant-a", "lease-2", 8]}
      ])

    assert {_refreshed, {:decoded, :charged}} =
             Flow.step(first_client, claimed_job(),
               name: @step_name,
               run: fn -> :charged end,
               to_state: "schedule_warning",
               codec: NormalizingCodec
             )

    assert_receive {:request, _extend_opcode, _route, _payload, []}

    assert_receive {:request, _continue_opcode, _route, %{"values" => %{@step_key => ^encoded}},
                    []}

    {:ok, replay_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [encoded]}
      ])

    assert {_job, {:decoded, :charged}} =
             Flow.step(replay_client, claimed_job_at("schedule_warning"),
               name: @step_name,
               run: fn -> flunk("committed closure ran again") end,
               to_state: "schedule_warning",
               codec: NormalizingCodec
             )
  end

  test "step replays a committed named result without running or advancing" do
    result = %{charge_id: "ch_123", amount: 150}

    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [Term.encode(result)]}
      ])

    assert {job, ^result} =
             Flow.step(client, claimed_job_at("schedule_warning"),
               name: @step_name,
               run: fn -> flunk("committed closure ran again") end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert job == Map.put(claimed_job_at("schedule_warning"), "state", "running")

    assert_receive {:request, extend_opcode, _route, _payload, []}
    assert extend_opcode == Opcodes.flow_extend_lease()

    assert_receive {:request, value_opcode, _route, %{"refs" => ["value-ref"]}, []}
    assert value_opcode == Opcodes.flow_value_mget()
    continue_opcode = Opcodes.flow_step_continue()
    refute_receive {:request, ^continue_opcode, _route, _payload, _opts}
  end

  test "step distinguishes a committed nil result from a missing value" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [Term.encode(nil)]}
      ])

    assert {job, nil} =
             Flow.step(client, claimed_job_at("schedule_warning"),
               name: @step_name,
               run: fn -> flunk("committed nil closure ran again") end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert job == Map.put(claimed_job_at("schedule_warning"), "state", "running")
  end

  test "step fails closed when a committed result is not in the requested target state" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job(%{@step_key => "value-ref"})}
      ])

    assert {:error, %FerricStore.Error{}} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: fn -> flunk("mismatched committed closure ran again") end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert_receive {:request, opcode, _route, _payload, []}
    assert opcode == Opcodes.flow_extend_lease()
    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "a stale lease error is returned before the closure executes" do
    {:ok, client} = ScriptedClient.start_link(self(), [{:error, "ERR stale lease"}])

    assert {:error, %FerricStore.Error{message: message}} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: fn -> flunk("stale closure executed") end,
               to_state: "schedule_warning"
             )

    assert message =~ "stale lease"
    assert_receive {:request, opcode, _route, _payload, []}
    assert opcode == Opcodes.flow_extend_lease()
    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "a closure failure is not committed or advanced" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()}
      ])

    assert_raise RuntimeError, "provider failed", fn ->
      Flow.step(client, claimed_job(),
        name: @step_name,
        run: fn -> raise "provider failed" end,
        to_state: "schedule_warning"
      )
    end

    assert_receive {:request, opcode, _route, _payload, []}
    assert opcode == Opcodes.flow_extend_lease()
    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "a result encoding failure is returned without committing the step" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()}
      ])

    assert {:error,
            %FerricStore.Error{
              raw: {:flow_codec_encode_failed, FerricStore.Codec.Raw}
            }} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: fn -> %{not_raw_encodable: true} end,
               to_state: "schedule_warning"
             )

    assert_receive {:request, opcode, _route, _payload, []}
    assert opcode == Opcodes.flow_extend_lease()
    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "step rejects malformed lease-extension records before running the closure" do
    malformed = [
      Map.put(extended_job(), "id", "other-flow"),
      Map.put(extended_job(), "partition_key", "other-partition"),
      Map.put(extended_job(), "lease_token", "other-lease"),
      Map.put(extended_job(), "fencing_token", 8),
      Map.put(extended_job(), "state", "scheduled"),
      Map.put(extended_job(), "run_state", "other-state"),
      Map.delete(extended_job(), "state"),
      Map.delete(extended_job(), "run_state"),
      Map.put(extended_job(), "value_refs", nil),
      Map.put(extended_job(), "value_refs", [])
    ]

    for response <- malformed do
      {:ok, client} = ScriptedClient.start_link(self(), [{:ok, response}])

      assert {:error, %FerricStore.Error{}} =
               Flow.step(client, Map.put(claimed_job(), "state", "running"),
                 name: @step_name,
                 run: fn -> flunk("closure ran after malformed lease validation") end,
                 to_state: "schedule_warning"
               )

      assert_receive {:request, opcode, _route, _payload, []}
      assert opcode == Opcodes.flow_extend_lease()
    end
  end

  test "advance rejects malformed refreshed claims" do
    malformed = [
      ["other-flow", "tenant-a", "lease-2", 8],
      ["flow-1", "other-partition", "lease-2", 8],
      ["flow-1", "tenant-a", "lease-1", 8],
      ["flow-1", "tenant-a", "", 8],
      ["flow-1", "tenant-a", "lease-2", 7],
      ["flow-1", "tenant-a", "lease-2", 6],
      ["flow-1", "tenant-a", "lease-2", 8, "other-state"],
      refreshed_map() |> Map.delete("state"),
      refreshed_map() |> Map.put("state", "scheduled"),
      refreshed_map() |> Map.delete("run_state"),
      refreshed_map() |> Map.put("run_state", "other-state")
    ]

    for response <- malformed do
      {:ok, client} = ScriptedClient.start_link(self(), [{:ok, response}])

      assert {:error, error} = Flow.advance(client, claimed_job(), to_state: "schedule_warning")

      assert error.__struct__ ==
               FerricStore.Flow.DurableMutationOutcomeUnknownError

      assert_receive {:request, opcode, _route, _payload, []}
      assert opcode == Opcodes.flow_step_continue()
    end
  end

  test "active durable claims reject zero fences and explicit non-running physical state" do
    {:ok, client} = ScriptedClient.start_link(self(), [])

    for job <- [
          Map.put(claimed_job(), "fencing_token", 0),
          Map.put(claimed_job(), "state", "scheduled"),
          Map.put(claimed_job(), "state", ""),
          Map.put(claimed_job(), "state", nil)
        ] do
      assert {:error, %FerricStore.Error{}} =
               Flow.step(client, job,
                 name: @step_name,
                 run: fn -> flunk("invalid active claim closure ran") end,
                 to_state: "schedule_warning"
               )

      assert {:error, %FerricStore.Error{}} =
               Flow.advance(client, job, to_state: "schedule_warning")
    end

    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "STEP_CONTINUE transport and response uncertainty is explicit and never retried" do
    {:ok, transport_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:error, :closed}
      ])

    assert {:error, transport_error} =
             Flow.step(transport_client, claimed_job(),
               name: @step_name,
               run: fn ->
                 send(self(), :closure_executed)
                 :charged
               end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert transport_error.__struct__ ==
             FerricStore.Flow.DurableMutationOutcomeUnknownError

    assert transport_error.operation == :flow_step_continue
    assert %FerricStore.Error{raw: :closed} = transport_error.cause
    assert_receive {:request, extend_opcode, _route, _payload, []}
    assert extend_opcode == Opcodes.flow_extend_lease()
    assert_receive {:request, continue_opcode, _route, _payload, []}
    assert continue_opcode == Opcodes.flow_step_continue()
    assert_receive :closure_executed
    refute_receive :closure_executed
    refute_receive {:request, _opcode, _route, _payload, _opts}

    {:ok, response_client} =
      ScriptedClient.start_link(self(), [
        {:ok, ["flow-1", "tenant-a", "lease-1", 7]}
      ])

    assert {:error, response_error} =
             Flow.advance(response_client, claimed_job(), to_state: "schedule_warning")

    assert response_error.__struct__ ==
             FerricStore.Flow.DurableMutationOutcomeUnknownError

    assert response_error.operation == :flow_step_continue
    assert %FerricStore.Error{} = response_error.cause

    {:ok, http_client} =
      ScriptedClient.start_link(self(), [
        FerricStore.HTTP.Error.network(:econnreset)
      ])

    assert {:error, http_error} =
             Flow.advance(http_client, claimed_job(), to_state: "schedule_warning")

    assert http_error.__struct__ ==
             FerricStore.Flow.DurableMutationOutcomeUnknownError

    assert %FerricStore.HTTP.Error{reason: :econnreset} = http_error.cause
  end

  test "a definite STEP_CONTINUE server rejection is not marked outcome unknown" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:error, {:error, "ERR stale lease"}}
      ])

    assert {:error,
            %FerricStore.Error{
              status: :error,
              raw: "ERR stale lease"
            }} = Flow.advance(client, claimed_job(), to_state: "schedule_warning")

    {:ok, http_client} =
      ScriptedClient.start_link(self(), [
        {:error,
         %FerricStore.HTTP.Error{
           message: "unauthorized",
           status_code: 401,
           reason: {:http_status, 401}
         }}
      ])

    assert {:error, unauthorized_error} =
             Flow.advance(http_client, claimed_job(), to_state: "schedule_warning")

    assert unauthorized_error.__struct__ ==
             FerricStore.Flow.DurableMutationOutcomeUnknownError

    {:ok, command_error_client} =
      ScriptedClient.start_link(self(), [
        {:error, %{"code" => "stale_lease", "message" => "stale lease"}}
      ])

    assert {:error, %FerricStore.Error{raw: %{"code" => "stale_lease"}}} =
             Flow.advance(command_error_client, claimed_job(), to_state: "schedule_warning")

    {:ok, compatibility_error_client} =
      ScriptedClient.start_link(self(), [
        {:error,
         %{
           "code" => "error",
           "message" => "ERR stale flow lease",
           "safe_to_retry" => false
         }}
      ])

    assert {:error, %FerricStore.Error{raw: %{"code" => "error"}}} =
             Flow.advance(compatibility_error_client, claimed_job(), to_state: "schedule_warning")

    safe_reroute = %{"safe_to_retry" => true, "retryable" => true}

    {:ok, reroute_client} =
      ScriptedClient.start_link(self(), [{:error, {:reroute, safe_reroute}}])

    assert {:error, %FerricStore.Error{raw: {:reroute, ^safe_reroute}}} =
             Flow.advance(reroute_client, claimed_job(), to_state: "schedule_warning")

    unsafe_reroute = %{"safe_to_retry" => false, "retryable" => true}

    {:ok, unsafe_client} =
      ScriptedClient.start_link(self(), [{:error, {:reroute, unsafe_reroute}}])

    assert {:error, %FerricStore.Error{raw: {:reroute, ^unsafe_reroute}}} =
             Flow.advance(unsafe_client, claimed_job(), to_state: "schedule_warning")
  end

  test "ambiguous and future server failures remain outcome unknown without another write" do
    ambiguous = [
      {:error, {:error, %{"code" => "timeout", "message" => "Raft outcome unknown"}}},
      {:error, {:unknown_status, 77, %{"safe_to_retry" => true}}},
      {:error,
       %FerricStore.HTTP.Error{
         message: "request timed out after dispatch",
         status_code: 408,
         error_code: "request_timeout",
         reason: {:http_status, 408},
         retryable: true,
         safe_to_retry: true
       }}
    ]

    for reply <- ambiguous do
      {:ok, client} = ScriptedClient.start_link(self(), [reply])

      assert {:error, error} =
               Flow.advance(client, claimed_job(), to_state: "schedule_warning")

      assert error.__struct__ ==
               FerricStore.Flow.DurableMutationOutcomeUnknownError

      assert_receive {:request, opcode, _route, _payload, []}
      assert opcode == Opcodes.flow_step_continue()
      refute_receive {:request, ^opcode, _route, _payload, _opts}
    end
  end

  test "a proven local HTTP rejection is returned directly without retrying the mutation" do
    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:error,
         %FerricStore.HTTP.Error{
           message: "request exceeded the configured client limit",
           reason: :request_too_large,
           delivery: :not_sent
         }}
      ])

    assert {:error,
            %FerricStore.HTTP.Error{
              reason: :request_too_large,
              delivery: :not_sent
            }} = Flow.advance(client, claimed_job(), to_state: "schedule_warning")

    assert_receive {:request, opcode, _route, _payload, []}
    assert opcode == Opcodes.flow_step_continue()
    refute_receive {:request, ^opcode, _route, _payload, _opts}
  end

  test "a closed client is rejected before durable mutation submission" do
    {:ok, client} = ScriptedClient.start_link(self(), [])
    GenServer.stop(client)

    assert {:error, %FerricStore.Error{raw: :client_closed}} =
             Flow.advance(client, claimed_job(), to_state: "schedule_warning")

    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  test "durable closures execute in the caller-owned process" do
    caller = self()

    {:ok, client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:ok, ["flow-1", "tenant-a", "lease-2", 8]}
      ])

    assert {_job, :charged} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: fn ->
                 send(caller, {:closure_process, self()})
                 :charged
               end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert_receive {:closure_process, ^caller}
  end

  test "a fresh target-state claim safely replays after an uncertain commit response" do
    result = %{charge_id: "ch_123"}

    {:ok, uncertain_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job()},
        {:error, :closed}
      ])

    assert {:error, uncertain_error} =
             Flow.step(uncertain_client, claimed_job(),
               name: @step_name,
               run: fn -> result end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert uncertain_error.__struct__ ==
             FerricStore.Flow.DurableMutationOutcomeUnknownError

    {:ok, recovery_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [Term.encode(result)]}
      ])

    assert {recovered, ^result} =
             Flow.step(recovery_client, claimed_job_at("schedule_warning"),
               name: @step_name,
               run: fn -> flunk("committed closure reran during recovery") end,
               to_state: "schedule_warning",
               codec: Term
             )

    assert recovered["run_state"] == "schedule_warning"
    assert_receive {:request, extend_opcode, _route, _payload, []}
    assert extend_opcode == Opcodes.flow_extend_lease()
    assert_receive {:request, continue_opcode, _route, _payload, []}
    assert continue_opcode == Opcodes.flow_step_continue()
    assert_receive {:request, ^extend_opcode, _route, _payload, []}
    assert_receive {:request, value_opcode, _route, _payload, []}
    assert value_opcode == Opcodes.flow_value_mget()
    refute_receive {:request, ^continue_opcode, _route, _payload, _opts}
  end

  test "malformed committed references and values never run or advance the closure" do
    malformed = [
      [{:ok, Map.put(extended_job_at("schedule_warning"), "value_refs", [])}],
      [{:ok, extended_job_at("schedule_warning", %{@step_key => 123})}],
      [{:ok, extended_job_at("schedule_warning", %{@step_key => nil})}],
      [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, []}
      ],
      [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [nil]}
      ],
      [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, ["one", "two"]}
      ],
      [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, [123]}
      ]
    ]

    for replies <- malformed do
      {:ok, client} = ScriptedClient.start_link(self(), replies)

      assert {:error, %FerricStore.Error{}} =
               Flow.step(client, claimed_job_at("schedule_warning"),
                 name: @step_name,
                 run: fn -> flunk("closure ran after a malformed committed result") end,
                 to_state: "schedule_warning"
               )
    end

    {:ok, corrupt_client} =
      ScriptedClient.start_link(self(), [
        {:ok, extended_job_at("schedule_warning", %{@step_key => "value-ref"})},
        {:ok, ["not-an-external-term"]}
      ])

    assert {:error, %FerricStore.Error{}} =
             Flow.step(corrupt_client, claimed_job_at("schedule_warning"),
               name: @step_name,
               run: fn -> flunk("closure ran after a corrupt committed result") end,
               to_state: "schedule_warning",
               codec: Term
             )

    continue_opcode = Opcodes.flow_step_continue()
    refute_receive {:request, ^continue_opcode, _route, _payload, _opts}
  end

  test "invalid durable-step inputs fail before transport or closure execution" do
    {:ok, client} = ScriptedClient.start_link(self(), [])

    invalid_jobs = [
      Map.delete(claimed_job(), "lease_token"),
      Map.delete(claimed_job(), "run_state"),
      Map.put(claimed_job(), "fencing_token", -1)
    ]

    for job <- invalid_jobs do
      assert {:error, %FerricStore.Error{}} =
               Flow.step(client, job,
                 name: @step_name,
                 run: fn -> flunk("invalid closure executed") end,
                 to_state: "schedule_warning"
               )
    end

    assert {:error, %FerricStore.Error{}} =
             Flow.step(client, claimed_job(),
               name: "",
               run: fn -> flunk("invalid closure executed") end,
               to_state: "schedule_warning"
             )

    assert {:error, %FerricStore.Error{}} =
             Flow.step(client, claimed_job(),
               name: "   ",
               run: fn -> flunk("invalid closure executed") end,
               to_state: "schedule_warning"
             )

    assert {:error, %FerricStore.Error{}} =
             Flow.step(client, claimed_job(),
               name: <<"charge:", 0xFF>>,
               run: fn -> flunk("invalid closure executed") end,
               to_state: "schedule_warning"
             )

    assert {:error, %FerricStore.Error{}} =
             Flow.step(client, claimed_job(),
               name: @step_name,
               run: :not_a_function,
               to_state: "schedule_warning"
             )

    for forbidden <- [[worker: "worker-a"], [return: "RECORD"]] do
      assert {:error, %FerricStore.Error{}} =
               Flow.advance(
                 client,
                 claimed_job(),
                 [to_state: "schedule_warning"] ++ forbidden
               )
    end

    assert {:error, %FerricStore.Error{}} =
             Flow.advance(client, claimed_job(),
               to_state: "schedule_warning",
               lane_id: 1
             )

    refute_receive {:request, _opcode, _route, _payload, _opts}
  end

  defp claimed_job do
    %{
      "id" => "flow-1",
      "partition_key" => "tenant-a",
      "lease_token" => "lease-1",
      "fencing_token" => 7,
      "run_state" => "charge"
    }
  end

  defp claimed_job_at(run_state), do: Map.put(claimed_job(), "run_state", run_state)

  defp extended_job(value_refs \\ %{}) do
    claimed_job()
    |> Map.put("state", "running")
    |> Map.put("value_refs", value_refs)
  end

  defp extended_job_at(run_state, value_refs \\ %{}) do
    run_state
    |> claimed_job_at()
    |> Map.put("state", "running")
    |> Map.put("value_refs", value_refs)
  end

  defp refreshed_map do
    %{
      "id" => "flow-1",
      "partition_key" => "tenant-a",
      "lease_token" => "lease-2",
      "fencing_token" => 8,
      "state" => "running",
      "run_state" => "schedule_warning"
    }
  end
end
