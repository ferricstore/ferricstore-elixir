defmodule FerricStore.Flow.DurableStepTCPResponseLossTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK
  alias FerricStore.Test.NativeServer

  @tag capture_log: true
  test "a durable mutation is outcome unknown and is not replayed after TCP response loss" do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)
    {:ok, port_holder} = Agent.start_link(fn -> nil end)

    response_fun = fn request ->
      case request.opcode do
        0x0001 ->
          NativeServer.startup_payload()

        0x0007 ->
          NativeServer.topology_payload(Agent.get(port_holder, & &1))

        0x0222 ->
          Agent.update(attempts, &(&1 + 1))
          :close

        _other ->
          "OK"
      end
    end

    {:ok, server} = NativeServer.start_link(owner: self(), response_fun: response_fun)
    Agent.update(port_holder, fn _missing -> NativeServer.port(server) end)

    assert {:ok, client} =
             SDK.start_link(
               seeds: [{"127.0.0.1", NativeServer.port(server)}],
               connections_per_endpoint: 1
             )

    on_exit(fn ->
      SDK.close(client)
      if Process.alive?(server), do: GenServer.stop(server)
    end)

    job = %{
      "id" => "flow-1",
      "partition_key" => "tenant-a",
      "lease_token" => "lease-1",
      "fencing_token" => 7,
      "run_state" => "charge"
    }

    assert {:error, %FerricStore.Flow.DurableMutationOutcomeUnknownError{}} =
             FerricStore.Flow.advance(client, job,
               to_state: "schedule_warning",
               timeout: 500
             )

    Process.sleep(50)
    assert Agent.get(attempts, & &1) == 1
  end
end
