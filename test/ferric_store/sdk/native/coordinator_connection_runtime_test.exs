defmodule FerricStore.SDK.Native.CoordinatorConnectionCleanupTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{ConnectionPool, CoordinatorConnectionCleanup}
  alias FerricStore.SDK.Native.Coordinator.State

  setup do
    start_supervised!({DynamicSupervisor, strategy: :one_for_one})
    |> then(&%{connection_supervisor: &1})
  end

  test "stale refresh results stop newly created untracked connections", %{
    connection_supervisor: supervisor
  } do
    connection = start_connection(supervisor)
    monitor = Process.monitor(connection)
    state = %State{connection_supervisor: supervisor}

    assert {:noreply, ^state} =
             CoordinatorConnectionCleanup.discard_refresh(
               state,
               {:ok, %{}, connection, :endpoint, %{}, nil}
             )

    assert_receive {:DOWN, ^monitor, :process, ^connection, :shutdown}, 250
  end

  test "stale refresh results preserve a reused tracked connection", %{
    connection_supervisor: supervisor
  } do
    connection = start_connection(supervisor)
    state = %State{connection_supervisor: supervisor}
    {:ok, pool} = ConnectionPool.track(state.connection_pool, :endpoint, connection)
    state = %{state | connection_pool: pool}

    assert {:noreply, ^state} =
             CoordinatorConnectionCleanup.discard_refresh(
               state,
               {:ok, %{}, connection, :endpoint, %{}, nil}
             )

    assert Process.alive?(connection)
  end

  defp start_connection(supervisor) do
    {:ok, connection} =
      DynamicSupervisor.start_child(supervisor, {Agent, fn -> :connection end})

    connection
  end
end
