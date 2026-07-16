defmodule FerricStore.QueueTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Term
  alias FerricStore.Protocol
  alias FerricStore.Queue
  alias FerricStore.Test.ClientRuntime
  alias FerricStore.Test.ExplodingInspect

  defmodule CaptureClient do
    use GenServer

    def start_link(owner, claim_reply),
      do:
        GenServer.start_link(__MODULE__, {owner, claim_reply})
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
          {owner, _claim_reply} = state
        ) do
      opts = opts |> FerricStore.RequestContext.options() |> Keyword.delete(:key)
      send(owner, {:command, command, args, opts})
      {:reply, {:ok, "OK"}, state}
    end

    def handle_call({:command, opcode, _key, payload, opts}, from, state),
      do: handle_call({:request, opcode, payload, opts}, from, state)

    def handle_call({:request, opcode, payload, opts}, _from, {owner, claim_reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})

      reply =
        if opcode == Protocol.opcode(:flow_claim_due) do
          claim_reply
        else
          "OK"
        end

      case reply do
        {:error, reason} -> {:reply, {:error, reason}, state}
        value -> {:reply, {:ok, value}, state}
      end
    end

    def handle_call({:native, opcode, payload, opts}, _from, {owner, claim_reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:native, opcode, payload, opts})

      reply =
        if opcode == Protocol.opcode(:flow_claim_due) do
          claim_reply
        else
          "OK"
        end

      {:reply, reply, state}
    end

    def handle_call({:command, command, args, opts}, _from, {owner, _claim_reply} = state) do
      opts = FerricStore.RequestContext.options(opts)
      send(owner, {:command, command, args, opts})
      {:reply, "OK", state}
    end
  end

  test "run_once returns a claim error without invoking the handler" do
    {:ok, client} = CaptureClient.start_link(self(), {:error, :server_down})
    queue = Queue.new(client, "email")

    assert {:error, %FerricStore.Error{raw: :server_down}} =
             Queue.run_once(queue, fn job ->
               send(self(), {:handled, job})
               :unexpected
             end)

    refute_received {:handled, _job}
    refute_received {:command, _command, _args, _opts}
  end

  test "constructor rejects unknown, duplicate, and positional override options" do
    assert_raise ArgumentError, ~r/unknown keys.*typo/, fn ->
      Queue.new(self(), "email", typo: true)
    end

    assert_raise ArgumentError, ~r/duplicate keys.*worker/, fn ->
      Queue.new(self(), "email", worker: "one", worker: "two")
    end

    assert_raise ArgumentError, ~r/unknown keys.*client.*type/, fn ->
      Queue.new(self(), "email", client: self(), type: "other")
    end

    assert_raise ArgumentError, ~r/lease_ms.*exact positive integer/, fn ->
      Queue.new(self(), "email", lease_ms: 9_007_199_254_740_992)
    end
  end

  test "complete and fail carry the claimed partition key" do
    {:ok, client} = CaptureClient.start_link(self(), [])
    queue = Queue.new(client, "email")

    job = %{
      "id" => "job-1",
      "partition_key" => "tenant:explicit",
      "lease_token" => "lease",
      "fencing_token" => 7
    }

    assert "OK" = Queue.complete(queue, job, result: "done")

    assert_received {:native, opcode,
                     %{
                       "id" => "job-1",
                       "partition_key" => "tenant:explicit",
                       "lease_token" => "lease",
                       "fencing_token" => 7
                     }, []}

    assert opcode == Protocol.opcode(:flow_complete)

    assert "OK" = Queue.fail(queue, job, error: "failed")

    assert_received {:native, fail_opcode,
                     %{
                       "id" => "job-1",
                       "partition_key" => "tenant:explicit",
                       "lease_token" => "lease",
                       "fencing_token" => 7,
                       "error" => "failed"
                     }, []}

    assert fail_opcode == Protocol.opcode(:flow_fail)
  end

  test "claim hydrates and decodes payloads with the queue codec" do
    encoded = Term.encode(%{email: "user@example.com"})

    record = %{
      "id" => "job-1",
      "partition_key" => "tenant:explicit",
      "lease_token" => "lease",
      "fencing_token" => 7,
      "payload" => encoded
    }

    {:ok, client} = CaptureClient.start_link(self(), [record])
    queue = Queue.new(client, "email", codec: Term)

    assert [%{"payload" => %{email: "user@example.com"}}] = Queue.claim(queue)

    assert_received {:native, opcode,
                     %{
                       "payload" => true,
                       "return" => "RECORDS",
                       "type" => "email"
                     }, []}

    assert opcode == Protocol.opcode(:flow_claim_due)
  end

  test "false handler results are preserved in the completion payload" do
    job = ["job-1", "tenant:explicit", "lease", 7, %{}]
    {:ok, client} = CaptureClient.start_link(self(), [job])
    queue = Queue.new(client, "email")

    assert ["OK"] = Queue.run_once(queue, fn _job -> false end)

    assert_received {:native, opcode, %{"result" => "false"}, []}
    assert opcode == Protocol.opcode(:flow_complete)
  end

  test "run_once preserves the caller timeout budget for terminal requests" do
    job = ["job-1", "tenant:explicit", "lease", 7, %{}]
    {:ok, client} = CaptureClient.start_link(self(), [job])
    queue = Queue.new(client, "email")

    assert ["OK"] = Queue.run_once(queue, fn _job -> :done end, call_timeout: 3_210)

    assert_received {:native, claim_opcode, _payload, [call_timeout: 3_210]}
    assert claim_opcode == Protocol.opcode(:flow_claim_due)

    assert_received {:native, complete_opcode, _payload, [call_timeout: 3_210]}
    assert complete_opcode == Protocol.opcode(:flow_complete)
  end

  test "run_once starts a claimed batch with bounded concurrency" do
    jobs = [
      ["job-1", "tenant:one", "lease-1", 1, %{}],
      ["job-2", "tenant:two", "lease-2", 2, %{}]
    ]

    {:ok, client} = CaptureClient.start_link(self(), jobs)
    queue = Queue.new(client, "email")
    owner = self()

    task =
      Task.async(fn ->
        Queue.run_once(
          queue,
          fn job ->
            send(owner, {:handler_started, job["id"], self()})

            receive do
              :release_handler -> job["id"]
            end
          end,
          limit: 2,
          max_concurrency: 2
        )
      end)

    assert_receive {:handler_started, "job-1", first_handler}, 500
    assert_receive {:handler_started, "job-2", second_handler}, 500
    refute first_handler == second_handler

    send(first_handler, :release_handler)
    send(second_handler, :release_handler)

    assert ["OK", "OK"] = Task.await(task)
  end

  test "run_once fails a raised handler and continues settling the claimed batch" do
    jobs = [
      ["job-1", "tenant:one", "lease-1", 1, %{}],
      ["job-2", "tenant:two", "lease-2", 2, %{}]
    ]

    {:ok, client} = CaptureClient.start_link(self(), jobs)
    queue = Queue.new(client, "email")

    assert ["OK", "OK"] =
             Queue.run_once(
               queue,
               fn
                 %{"id" => "job-1"} -> raise "provider failed"
                 %{"id" => "job-2"} -> "sent"
               end,
               limit: 2,
               max_concurrency: 2
             )

    assert_received {:native, fail_opcode,
                     %{
                       "id" => "job-1",
                       "error" => "handler exception: provider failed"
                     }, []}

    assert fail_opcode == Protocol.opcode(:flow_fail)

    assert_received {:native, complete_opcode,
                     %{
                       "id" => "job-2",
                       "result" => "sent"
                     }, []}

    assert complete_opcode == Protocol.opcode(:flow_complete)
  end

  test "run_once settles a job even when a thrown reason cannot be inspected" do
    job = ["job-1", "tenant:one", "lease-1", 1, %{}]
    {:ok, client} = CaptureClient.start_link(self(), [job])
    queue = Queue.new(client, "email")

    assert ["OK"] = Queue.run_once(queue, fn _job -> throw(%ExplodingInspect{}) end)

    assert_received {:native, opcode,
                     %{
                       "id" => "job-1",
                       "error" => "handler throw: <unrenderable>"
                     }, []}

    assert opcode == Protocol.opcode(:flow_fail)
  end

  test "run_once contains an untrappable handler exit and continues the claimed batch" do
    jobs = [
      ["job-1", "tenant:one", "lease-1", 1, %{}],
      ["job-2", "tenant:two", "lease-2", 2, %{}]
    ]

    {:ok, client} = CaptureClient.start_link(self(), jobs)
    queue = Queue.new(client, "email")
    owner = self()

    {_runner, monitor} =
      spawn_monitor(fn ->
        result =
          Queue.run_once(
            queue,
            fn
              %{"id" => "job-1"} -> Process.exit(self(), :kill)
              %{"id" => "job-2"} -> "sent"
            end,
            limit: 2,
            max_concurrency: 2
          )

        send(owner, {:queue_result, result})
      end)

    assert_receive {:queue_result, ["OK", "OK"]}, 500
    assert_receive {:DOWN, ^monitor, :process, _runner, :normal}, 500

    assert_received {:native, opcode,
                     %{
                       "id" => "job-1",
                       "error" => "handler task exit: killed"
                     }, []}

    assert opcode == Protocol.opcode(:flow_fail)

    assert_received {:native, complete_opcode,
                     %{
                       "id" => "job-2",
                       "result" => "sent"
                     }, []}

    assert complete_opcode == Protocol.opcode(:flow_complete)
  end

  test "run_once rejects an invalid or duplicate concurrency bound before claiming" do
    {:ok, client} = CaptureClient.start_link(self(), [])
    queue = Queue.new(client, "email")

    assert_raise ArgumentError, ~r/max_concurrency must be a positive integer/, fn ->
      Queue.run_once(queue, fn _job -> :ok end, max_concurrency: 0)
    end

    assert_raise ArgumentError, ~r/max_concurrency must be between 1 and 256/, fn ->
      Queue.run_once(queue, fn _job -> :ok end, max_concurrency: 257)
    end

    assert_raise ArgumentError, ~r/duplicate.*max_concurrency/, fn ->
      Queue.run_once(queue, fn _job -> :ok end,
        max_concurrency: 1,
        max_concurrency: 2
      )
    end

    refute_received {:native, _, _, _}
  end

  test "queue entry points bound option admission before merging or keyword scans" do
    {:ok, client} = CaptureClient.start_link(self(), [])
    queue = Queue.new(client, "email")
    options = List.duplicate({:limit, 1}, 100_000)

    {claim_reductions, claim_result} =
      measured_result_reductions(fn -> Queue.claim(queue, options) end)

    assert {:error,
            %FerricStore.Error{
              raw: {:too_many_flow_options, :claim_due, %{limit: 64, observed: 65}}
            }} = claim_result

    assert claim_reductions < 20_000

    {run_reductions, run_result} =
      measured_result_reductions(fn ->
        assert_raise ArgumentError, ~r/queue options exceed 65 entries/, fn ->
          Queue.run_once(queue, fn _job -> :ok end, options)
        end
      end)

    assert %ArgumentError{} = run_result
    assert run_reductions < 20_000
    refute_received {:native, _, _, _}
  end

  test "queue constructor bounds option admission before validation" do
    options = List.duplicate({:worker, "worker"}, 100_000)

    {reductions, result} =
      measured_result_reductions(fn ->
        assert_raise ArgumentError, ~r/consumer options exceed 16 entries/, fn ->
          Queue.new(self(), "email", options)
        end
      end)

    assert %ArgumentError{} = result
    assert reductions < 20_000
  end

  defp measured_result_reductions(function) do
    {:reductions, before_count} = Process.info(self(), :reductions)
    result = function.()
    {:reductions, after_count} = Process.info(self(), :reductions)
    {after_count - before_count, result}
  end
end
