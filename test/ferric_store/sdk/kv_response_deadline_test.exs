defmodule FerricStore.SDK.KVResponseDeadlineTest do
  use ExUnit.Case, async: false

  alias FerricStore.DeadlineBudget
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
          {:delayed, delay, response} = state
        ) do
      Process.sleep(delay)
      {:reply, {:ok, response}, state}
    end

    def handle_call({:command, _opcode, _key, _payload, _context}, _from, response),
      do: {:reply, {:ok, response}, response}

    def handle_call(
          {:command_items, _opcode, _items, _item_count, _key_fun, _payload_builder, _context},
          _from,
          {:delayed, delay, response} = state
        ) do
      Process.sleep(delay)
      {:reply, response, state}
    end
  end

  test "large list response validation stops at the absolute request deadline" do
    response = List.duplicate("value", 2_000_000)
    {:ok, client} = ReplyClient.start_link({:delayed, 10, response})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} = KV.lrange(client, "list", 0, -1, timeout: 15)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 500_000
  end

  test "oversized map responses are rejected without enumerating their entries" do
    response = Map.new(1..1_000_000, &{"field-#{&1}", "value"})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, {:invalid_kv_response, %{operation: :hgetall, reason: :collection_too_large}}} =
             Response.map({:ok, response}, :hgetall)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 20_000
  end

  test "grouped MGET validation stops at the absolute request deadline" do
    keys = List.duplicate("key", 100_000)
    indexes = Enum.to_list(0..99_999)
    values = List.duplicate("value", 2_000_000)
    response = {:ok, [%{indexes: indexes, value: values}]}
    {:ok, client} = ReplyClient.start_link({:delayed, 10, response})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :timeout} = KV.mget(client, keys, timeout: 15)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 600_000
  end

  test "grouped write validation honors an expired response budget" do
    indexes = Enum.to_list(0..99_999)
    budget = DeadlineBudget.new(0)

    for {operation, result} <- [
          {:del, fn -> BatchResults.del([%{indexes: indexes, value: 0}], 100_000, budget) end},
          {:mset,
           fn -> BatchResults.mset([%{indexes: indexes, value: "OK"}], 100_000, budget) end}
        ] do
      :erlang.garbage_collect(self())
      {:reductions, before_count} = Process.info(self(), :reductions)

      assert {:error, :timeout} = result.(), "#{operation} ignored the expired response budget"

      {:reductions, after_count} = Process.info(self(), :reductions)
      assert after_count - before_count < 10_000
    end
  end
end
