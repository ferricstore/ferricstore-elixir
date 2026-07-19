defmodule FerricStore.Flow.V091PolicyContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Flow.{PolicyCommand, PolicySnapshot, StalePolicyGenerationError}
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.Protocol.PipelineRequest
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{ClientRequestAdmission, RetryPolicy, ServerContract}
  alias FerricStore.Test.{ClientRuntime, NativeServer}
  alias FerricStore.Workflow

  @max_generation 9_007_199_254_740_991

  defmodule PolicyClient do
    use GenServer

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

    def handle_call({:request, opcode, payload, context}, _from, state) do
      send(state.owner, {:policy_request, opcode, payload, context})
      reply(state)
    end

    def handle_call({:command, opcode, _key, payload, context}, from, state),
      do: handle_call({:request, opcode, payload, context}, from, state)

    defp reply(%{replies: [reply | rest]} = state),
      do: {:reply, reply, %{state | replies: rest}}
  end

  test "direct policy updates default to deep patch and accept replacement CAS options" do
    assert {:ok, patch} = PolicyCommand.set_payload("email", max_active_ms: 30_000)
    refute Map.has_key?(patch, "replace")
    refute Map.has_key?(patch, "expected_generation")

    assert {:ok,
            %{
              "type" => "email",
              "replace" => true,
              "expected_generation" => @max_generation,
              "states" => %{"queued" => %{"mode" => :fifo}}
            }} =
             PolicyCommand.set_payload("email",
               replace: true,
               expected_generation: @max_generation,
               states: %{"queued" => [mode: :fifo]}
             )
  end

  test "replacement and generation validation is strict and bounded" do
    for value <- [nil, 0, 1, "true", :replace] do
      assert {:error, {:invalid_policy_option, "replace"}} =
               PolicyCommand.set_payload("email", replace: value)
    end

    for value <- [-1, @max_generation + 1, 1.0, "1", :infinity] do
      assert {:error, {:invalid_policy_option, "expected_generation"}} =
               PolicyCommand.set_payload("email", expected_generation: value)
    end

    assert {:ok, %{"expected_generation" => 0}} =
             PolicyCommand.set_payload("email", expected_generation: 0)
  end

  test "high-level and typed SDK policy APIs return policy snapshots" do
    response = policy_response("email", 7, %{"max_active_ms" => 30_000})
    {:ok, client} = PolicyClient.start_link(self(), [{:ok, response}, {:ok, response}])

    assert %PolicySnapshot{
             type: "email",
             generation: 7,
             max_active_ms: 30_000,
             states: %{"queued" => %{"mode" => "fifo"}}
           } = Flow.policy_set(client, "email", max_active_ms: 30_000)

    assert {:ok, %PolicySnapshot{type: "email", generation: 7}} =
             FerricStore.SDK.Flow.policy_get(client, %{type: "email"})

    assert_received {:policy_request, opcode, %{"type" => "email", "max_active_ms" => 30_000},
                     _context}

    assert opcode == Opcodes.flow_policy_set()
  end

  test "policy snapshots validate safe generations and preserve future extensions" do
    response =
      "email"
      |> policy_response(@max_generation)
      |> Map.put("future_policy_field", %{"enabled" => true})

    assert {:ok,
            %PolicySnapshot{
              generation: @max_generation,
              extensions: %{"future_policy_field" => %{"enabled" => true}}
            }} = PolicySnapshot.decode(response, "email")

    for invalid <- [-1, @max_generation + 1, 1.0, "1", nil] do
      assert {:error, {:invalid_policy_snapshot, %{field: :generation, value: ^invalid}}} =
               response
               |> Map.put("generation", invalid)
               |> PolicySnapshot.decode("email")
    end
  end

  test "stale policy generations return a dedicated error" do
    stale =
      {:error,
       {:error,
        %{
          "code" => "error",
          "message" => "ERR stale flow policy generation",
          "retryable" => false,
          "safe_to_retry" => false,
          "retry_after_ms" => 0
        }}}

    {:ok, high_level_client} = PolicyClient.start_link(self(), [stale])

    assert {:error,
            %StalePolicyGenerationError{
              expected_generation: 4,
              message: "ERR stale flow policy generation"
            }} =
             Flow.policy_set(high_level_client, "email",
               expected_generation: 4,
               max_active_ms: 30_000
             )

    {:ok, typed_client} = PolicyClient.start_link(self(), [stale])

    assert {:error, %StalePolicyGenerationError{expected_generation: 4}} =
             FerricStore.SDK.Flow.policy_set(typed_client, %{
               type: "email",
               expected_generation: 4,
               max_active_ms: 30_000
             })
  end

  test "CAS policy mutations are never automatically retried" do
    opcode = Opcodes.flow_policy_set()
    busy = {:busy, %{"retryable" => true, "safe_to_retry" => true, "retry_after_ms" => 1}}
    context = RequestContext.new([], 5_000)

    assert {:ok, patch_context} =
             ClientRequestAdmission.prepare_context(
               opcode,
               %{"type" => "email", "max_active_ms" => 30_000},
               context
             )

    assert RetryPolicy.retryable?(busy, opcode, patch_context)

    assert {:ok, cas_context} =
             ClientRequestAdmission.prepare_context(
               opcode,
               %{"type" => "email", "expected_generation" => 3, "max_active_ms" => 30_000},
               context
             )

    refute RetryPolicy.retryable?(busy, opcode, cas_context)
    refute RetryPolicy.retryable?({:connect_failed, :econnrefused}, opcode, cas_context)
  end

  test "CAS policy mutations nested in pipelines are never automatically retried" do
    command = %{
      opcode: Opcodes.flow_policy_set(),
      body: %{"type" => "email", "expected_generation" => 3, "max_active_ms" => 30_000}
    }

    {:ok, client} = PolicyClient.start_link(self(), List.duplicate({:ok, []}, 3))

    assert [] = FerricStore.pipeline(client, [command])

    assert_received {:policy_request, opcode, %PipelineRequest{}, context}
    assert opcode == Opcodes.pipeline()
    refute context.automatic_retry
    refute RetryPolicy.retryable?({:connect_failed, :econnrefused}, opcode, context)

    patch = %{opcode: Opcodes.flow_policy_set(), body: %{"type" => "email"}}
    assert [] = FerricStore.pipeline(client, [patch])
    assert_received {:policy_request, ^opcode, %PipelineRequest{}, patch_context}
    assert patch_context.automatic_retry

    raw_cas = ["FLOW.POLICY.SET", "email", "EXPECTED_GENERATION", "3"]
    assert [] = FerricStore.pipeline(client, [raw_cas])
    assert_received {:policy_request, ^opcode, %PipelineRequest{}, raw_context}
    refute raw_context.automatic_retry
  end

  test "raw policy CAS mutations are never automatically retried" do
    {:ok, client} = PolicyClient.start_link(self(), [{:ok, "OK"}])

    assert "OK" =
             FerricStore.command(client, "FLOW.POLICY.SET", [
               "email",
               "EXPECTED_GENERATION",
               "3"
             ])

    assert_received {:policy_request, opcode, _payload, context}
    assert opcode == Opcodes.command_exec()
    refute context.automatic_retry
    refute RetryPolicy.retryable?({:connect_failed, :econnrefused}, opcode, context)
  end

  test "workflow policy installation replaces declarations by default" do
    response = policy_response("email", 1)
    {:ok, client} = PolicyClient.start_link(self(), [{:ok, response}, {:ok, response}])
    workflow = Workflow.new(client, "email")

    assert %PolicySnapshot{generation: 1} =
             Workflow.install_policy(workflow, states: %{"queued" => [mode: :fifo]})

    assert_received {:policy_request, opcode,
                     %{
                       "type" => "email",
                       "replace" => true,
                       "states" => %{"queued" => %{"mode" => :fifo}}
                     }, _context}

    assert opcode == Opcodes.flow_policy_set()

    assert %PolicySnapshot{generation: 1} =
             Workflow.install_policy(workflow, replace: false, max_active_ms: 30_000)

    assert_received {:policy_request, ^opcode,
                     %{"type" => "email", "replace" => false, "max_active_ms" => 30_000},
                     _context}
  end

  test "HELLO requires both v0.9.1 policy capability fields" do
    fields =
      NativeServer.startup_payload()
      |> get_in(["capabilities", "schemas", "FLOW.POLICY.SET", "fields"])

    assert "replace" in fields
    assert "expected_generation" in fields

    for field <- ["replace", "expected_generation"] do
      startup =
        NativeServer.startup_payload()
        |> put_in(
          ["capabilities", "schemas", "FLOW.POLICY.SET", "fields"],
          fields -- [field]
        )

      assert {:error,
              {:incompatible_server_contract,
               %{command: "FLOW.POLICY.SET", missing_supported_fields: [^field]}}} =
               ServerContract.validate(startup)
    end
  end

  defp policy_response(type, generation, overrides \\ %{}) do
    Map.merge(
      %{
        "type" => type,
        "generation" => generation,
        "version" => nil,
        "max_active_ms" => "infinity",
        "retry" => %{},
        "retention" => %{},
        "indexed_attributes" => [],
        "indexed_state_meta" => nil,
        "governance" => nil,
        "states" => %{"queued" => %{"mode" => "fifo"}}
      },
      overrides
    )
  end
end
