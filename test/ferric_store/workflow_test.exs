defmodule FerricStore.WorkflowTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Protocol
  alias FerricStore.Test.ClientRuntime
  alias FerricStore.Workflow

  defmodule CaptureClient do
    use GenServer

    def start_link(owner, reply),
      do:
        GenServer.start_link(__MODULE__, {owner, reply})
        |> ClientRuntime.wrap()

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({:command, opcode, _key, payload, opts}, from, state),
      do: handle_call({:request, opcode, payload, opts}, from, state)

    def handle_call({:request, opcode, payload, opts}, _from, {owner, replies}) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})
      {reply, replies} = next_reply(replies)
      {:reply, {:ok, reply}, {owner, replies}}
    end

    def handle_call({:native, opcode, payload, opts}, _from, {owner, reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})
      {:reply, reply, state}
    end

    defp next_reply({:script, [reply | replies]}), do: {reply, {:script, replies}}
    defp next_reply(reply), do: {reply, reply}
  end

  test "claim hydrates and decodes records with the workflow codec" do
    encoded = Term.encode(%{order: 42})

    {:ok, client} =
      CaptureClient.start_link(self(), [
        %{
          "id" => "order-42",
          "lease_token" => "lease",
          "fencing_token" => 3,
          "payload" => encoded
        }
      ])

    workflow = Workflow.new(client, "order", codec: Term)

    assert [%{"payload" => %{order: 42}}] = Workflow.claim(workflow, "created")

    assert_received {:native, opcode,
                     %{"payload" => true, "return" => "RECORDS", "state" => "created"}, []}

    assert opcode == Protocol.opcode(:flow_claim_due)
  end

  test "constructor rejects unknown, duplicate, and positional override options" do
    assert_raise ArgumentError, ~r/unknown keys.*typo/, fn ->
      Workflow.new(self(), "order", typo: true)
    end

    assert_raise ArgumentError, ~r/duplicate keys.*worker/, fn ->
      Workflow.new(self(), "order", worker: "one", worker: "two")
    end

    assert_raise ArgumentError, ~r/unknown keys.*client.*type/, fn ->
      Workflow.new(self(), "order", client: self(), type: "other")
    end

    assert_raise ArgumentError, ~r/lease_ms.*exact positive integer/, fn ->
      Workflow.new(self(), "order", lease_ms: 9_007_199_254_740_992)
    end
  end

  test "workflow entry points bound option admission before merging defaults" do
    {:ok, client} = CaptureClient.start_link(self(), "OK")
    workflow = Workflow.new(client, "order")
    options = List.duplicate({:payload, "body"}, 100_000)

    {:reductions, before_count} = Process.info(self(), :reductions)
    result = Workflow.start(workflow, "order-1", options)
    {:reductions, after_count} = Process.info(self(), :reductions)

    assert {:error,
            %FerricStore.Error{
              raw: {:too_many_flow_options, :create, %{limit: 64, observed: 65}}
            }} = result

    assert after_count - before_count < 20_000
    refute_received {:native, _, _, _}
  end

  test "workflow entry points return typed errors for malformed options" do
    {:ok, client} = CaptureClient.start_link(self(), "OK")
    workflow = Workflow.new(client, "order")

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_options, :create, :expected_keyword}}} =
             Workflow.start(workflow, "order-1", :not_options)

    refute_received {:native, _, _, _}
  end

  test "advance and step inherit the workflow lease and codec" do
    result = %{charge_id: "ch_123"}

    {:ok, client} =
      CaptureClient.start_link(
        self(),
        {:script,
         [
           claimed_job() |> Map.put("state", "running") |> Map.put("value_refs", %{}),
           ["flow-1", "tenant-a", "lease-2", 8],
           ["flow-1", "tenant-a", "lease-3", 9]
         ]}
      )

    workflow = Workflow.new(client, "billing", codec: Term, lease_ms: 45_000)

    assert {stepped, ^result} =
             Workflow.step(workflow, claimed_job(),
               name: "charge-customer:v1",
               run: fn -> result end,
               to_state: "schedule_warning"
             )

    assert stepped["run_state"] == "schedule_warning"

    assert_receive {:native, extend_opcode, %{"lease_ms" => 45_000}, []}
    assert extend_opcode == Protocol.opcode(:flow_extend_lease)

    assert_receive {:native, continue_opcode,
                    %{
                      "lease_ms" => 45_000,
                      "values" => values,
                      "return" => "JOBS_COMPACT"
                    }, []}

    assert continue_opcode == Protocol.opcode(:flow_step_continue)
    assert [encoded] = Map.values(values)
    assert Term.decode(encoded) == result

    assert advanced =
             Workflow.advance(workflow, stepped, to_state: "warning_scheduled")

    assert advanced["run_state"] == "warning_scheduled"
    assert advanced["lease_token"] == "lease-3"

    assert_receive {:native, ^continue_opcode,
                    %{"lease_ms" => 45_000, "return" => "JOBS_COMPACT"}, []}
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
end
