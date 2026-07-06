defmodule FerricStore.SDK.FlowTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Flow
  alias FerricStore.SDK.Native.Opcodes

  defmodule CaptureClient do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:request, opcode, payload, opts}, _from, test_pid) do
      send(test_pid, {:request, opcode, payload, opts})
      {:reply, {:ok, payload}, test_pid}
    end

    def handle_call({:command, opcode, key, payload, opts}, _from, test_pid) do
      send(test_pid, {:request_by_key, opcode, key, payload, opts})
      {:reply, {:ok, payload}, test_pid}
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

    assert_received {:request_by_key, opcode, "tenant:a",
                     %{
                       "type" => "review",
                       "partition_key" => "tenant:a",
                       "terminal_only" => true
                     }, []}

    assert opcode == Opcodes.flow_search()
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

    assert_received {:request_by_key, opcode, "tenant:a:budget",
                     %{"scope" => "tenant:a:budget", "amount" => 10}, []}

    assert opcode == Opcodes.flow_budget_release()
  end
end
