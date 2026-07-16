defmodule FerricStore.SDK.Native.CoordinatorConnectionCleanup do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionLifecycle, ConnectionPool}
  alias FerricStore.SDK.Native.Coordinator.State

  @spec discard_start(State.t(), term()) :: {:noreply, State.t()}
  def discard_start(state, {:ok, connection, _capacity}) when is_pid(connection) do
    ConnectionLifecycle.stop(state.connection_supervisor, connection)
    {:noreply, state}
  end

  def discard_start(state, _result), do: {:noreply, state}

  @spec discard_refresh(State.t(), term()) :: {:noreply, State.t()}
  def discard_refresh(
        state,
        {:ok, _topology, connection, _key, _capacity, _replaced_connection}
      )
      when is_pid(connection) do
    unless ConnectionPool.connection?(state.connection_pool, connection) do
      ConnectionLifecycle.stop(state.connection_supervisor, connection)
    end

    {:noreply, state}
  end

  def discard_refresh(state, _result), do: {:noreply, state}
end
