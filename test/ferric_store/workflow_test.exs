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

    def handle_call({:request, opcode, payload, opts}, _from, {owner, reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})
      {:reply, {:ok, reply}, state}
    end

    def handle_call({:native, opcode, payload, opts}, _from, {owner, reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})
      {:reply, reply, state}
    end
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
end
