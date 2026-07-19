defmodule FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionLifecycle,
    CoordinatorConnectionAcquisition,
    CoordinatorConnectionCleanup,
    CoordinatorConnectionRuntime,
    RefreshOperation,
    TopologyInitialization,
    TopologyManager,
    TopologyRefreshCompletions,
    TopologyRefreshStarter,
    TopologyRefreshWaiter,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec initialize(State.t()) :: {:ok, State.t()} | {:error, term()}
  def initialize(state) do
    TopologyInitialization.run(
      state,
      &CoordinatorConnectionRuntime.track/4,
      &CoordinatorConnectionAcquisition.warm/1,
      &State.put_event_connection/2
    )
  end

  @spec start(State.t(), RefreshOperation.waiter(), map()) :: {:noreply, State.t()}
  def start(state, waiter, callbacks) do
    case operation(state) do
      nil -> start_new(state, waiter, callbacks)
      operation -> add_to_operation(state, operation, waiter)
    end
  end

  @spec finish(State.t(), RefreshOperation.t() | [RefreshOperation.waiter()], term(), map()) ::
          {:noreply, State.t()}
  def finish(
        state,
        completion_source,
        {:ok, topology, conn, key, capacity, replaced_connection},
        callbacks
      ) do
    case TopologyRuntime.put(state, topology) do
      {:ok, published_state} ->
        state =
          published_state
          |> retire_replaced_connection(replaced_connection)
          |> CoordinatorConnectionRuntime.track(key, conn, capacity)
          |> TopologyRuntime.prune()
          |> callbacks.maybe_start_event_restore.(conn)
          |> CoordinatorConnectionAcquisition.warm()

        {:noreply, TopologyRefreshCompletions.enqueue(state, completion_source, :ok)}

      {:error, reason} ->
        result = {:ok, topology, conn, key, capacity, replaced_connection}
        {:noreply, state} = CoordinatorConnectionCleanup.discard_refresh(state, result)
        finish(state, completion_source, {:error, reason}, callbacks)
    end
  end

  def finish(state, completion_source, {:error, reason}, _callbacks) do
    {:noreply, TopologyRefreshCompletions.enqueue(state, completion_source, {:error, reason})}
  end

  @spec finish_waiter(RefreshOperation.waiter(), term(), State.t(), map()) :: State.t()
  def finish_waiter(waiter, result, state, callbacks),
    do: TopologyRefreshWaiter.finish(waiter, result, state, callbacks, &start/3)

  @spec refresher_down(State.t(), RefreshOperation.t(), term(), map()) ::
          {:noreply, State.t()}
  def refresher_down(state, operation, reason, callbacks) do
    state = state |> put_operation(nil) |> release(operation)
    finish(state, operation, {:error, {:topology_refresh_failed, reason}}, callbacks)
  end

  @spec cancel(State.t(), term()) :: State.t()
  def cancel(state, key) do
    case operation(state) do
      nil ->
        TopologyRefreshCompletions.cancel(state, key)

      operation ->
        cancel_active(state, operation, key)
    end
  end

  @spec operation(State.t()) :: RefreshOperation.t() | nil
  def operation(state), do: TopologyManager.refresh_operation(state.topology_manager)

  @spec put_operation(State.t(), RefreshOperation.t() | nil) :: State.t()
  def put_operation(state, operation) do
    manager = TopologyManager.put_refresh_operation(state.topology_manager, operation)
    %{state | topology_manager: manager}
  end

  @spec release(State.t(), RefreshOperation.t()) :: State.t()
  def release(%{connection_pool: pool} = state, operation) do
    %{state | connection_pool: TopologyRefreshStarter.release(pool, operation)}
  end

  defp start_new(state, waiter, callbacks) do
    opts = [
      owner: self(),
      connection_supervisor: state.connection_supervisor,
      operation_supervisor: state.operation_supervisor,
      endpoint_policy: state.endpoint_policy,
      endpoint_trust: state.endpoint_trust,
      username: state.username,
      password: state.password,
      client_name: state.client_name,
      endpoint_validator: state.endpoint_validator,
      timeout: state.topology_refresh_timeout
    ]

    case TopologyRefreshStarter.start(
           TopologyRuntime.candidates(state),
           state.connection_pool,
           opts
         ) do
      {:ok, operation, pool} ->
        state =
          state
          |> Map.put(:connection_pool, pool)
          |> State.put_lifecycle_monitor(operation.monitor, {:topology_refresh, operation.token})

        add_to_operation(state, operation, waiter)

      {:error, reason, pool} ->
        finish(%{state | connection_pool: pool}, [waiter], {:error, reason}, callbacks)
    end
  end

  defp add_to_operation(state, operation, waiter) do
    {operation, _added?} = RefreshOperation.add(operation, waiter)
    {:noreply, put_operation(state, operation)}
  end

  defp cancel_active(state, operation, key) do
    case RefreshOperation.cancel(operation, key) do
      :empty ->
        Process.demonitor(operation.monitor, [:flush])
        TopologyRefreshStarter.cancel(operation)

        state
        |> State.delete_lifecycle_monitor(operation.monitor, {:topology_refresh, operation.token})
        |> put_operation(nil)
        |> release(operation)

      {:ok, operation} ->
        put_operation(state, operation)

      :missing ->
        TopologyRefreshCompletions.cancel(state, key)
    end
  end

  defp retire_replaced_connection(state, conn) when is_pid(conn) do
    %{state | connection_pool: ConnectionLifecycle.retire(state.connection_pool, conn)}
  end

  defp retire_replaced_connection(state, _connection), do: state
end
