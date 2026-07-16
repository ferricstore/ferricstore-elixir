defmodule FerricStore.SDK.Native.CoordinatorShutdownTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{Coordinator, CoordinatorShutdown, TopologyManager}

  test "shutdown replies to refresh callers whose result is already queued" do
    reply_tag = make_ref()
    caller_monitor = Process.monitor(self())

    waiter =
      {:refresh_call, {self(), reply_tag}, caller_monitor, nil,
       FerricStore.RequestContext.new([], 100)}

    manager =
      %TopologyManager{}
      |> TopologyManager.enqueue_refresh_waiters([waiter], :ok)

    state = %Coordinator.State{topology_manager: manager}

    assert :ok = CoordinatorShutdown.run(state, :client_closed)
    assert_receive {^reply_tag, {:error, :client_closed}}
  end
end
