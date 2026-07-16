defmodule FerricStore.SDK.Native.CoordinatorConnectionRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionLifecycle,
    ConnectionPool,
    CoordinatorTimers,
    RequestRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type waiter_removal :: {:ok, State.t()} | {:capacity_released, State.t()}

  @spec remove_waiter(State.t(), term() | nil, term()) :: waiter_removal()
  def remove_waiter(state, nil, _tag), do: {:ok, state}

  def remove_waiter(state, key, tag) do
    case ConnectionPool.remove_waiter(state.connection_pool, key, tag) do
      {:missing, pool} ->
        {:ok, %{state | connection_pool: pool}}

      {:remaining, pool} ->
        {:ok, %{state | connection_pool: pool}}

      {:empty, attempt, pool} ->
        CoordinatorTimers.demonitor(attempt.monitor)
        ConnectionLifecycle.stop(state.operation_supervisor, attempt.starter)

        state =
          state
          |> Map.put(:connection_pool, pool)
          |> State.delete_lifecycle_monitor(
            attempt.monitor,
            {:connection_attempt, attempt.key}
          )

        {:capacity_released, state}
    end
  end

  @spec track(State.t(), term(), pid(), map()) :: State.t()
  def track(state, key, connection, capacity) when is_pid(connection) do
    case ConnectionLifecycle.track(
           state.connection_pool,
           state.lifecycle_registry,
           key,
           connection,
           capacity
         ) do
      {:ok, pool, lifecycle_registry} ->
        %{state | connection_pool: pool, lifecycle_registry: lifecycle_registry}

      {:error, :connection_backpressure, _pool, _lifecycle_registry} ->
        ConnectionLifecycle.stop(state.connection_supervisor, connection)
        state
    end
  end

  @spec fail_requests(State.t(), pid(), term(), (State.t(), pid(), reference(), term() -> term())) ::
          State.t()
  def fail_requests(state, connection, reason, handle_response)
      when is_pid(connection) and is_function(handle_response, 4) do
    failure = {:transport_failed, {:connection_down, reason}}

    state.request_registry
    |> RequestRegistry.connection_tags(connection)
    |> Enum.reduce(state, fn tag, state ->
      {:noreply, state} = handle_response.(state, connection, tag, {:error, failure})
      state
    end)
  end

  @spec handle_attempt_down(
          State.t(),
          term(),
          reference(),
          term(),
          (State.t(), map(), term() -> {:noreply, State.t()})
        ) :: {:noreply, State.t()}
  def handle_attempt_down(state, key, monitor, reason, complete_attempt)
      when is_reference(monitor) and is_function(complete_attempt, 3) do
    case ConnectionPool.pop_attempt(state.connection_pool, key) do
      {%{monitor: ^monitor} = attempt, pool} ->
        complete_attempt.(
          %{state | connection_pool: pool},
          attempt,
          {:error, {:connect_failed, reason}}
        )

      {attempt, pool} ->
        pool = if attempt, do: ConnectionPool.put_attempt(pool, key, attempt), else: pool
        {:noreply, %{state | connection_pool: pool}}
    end
  end

  @spec put_waiter_connection_key(State.t(), term(), term()) :: State.t()
  def put_waiter_connection_key(state, tag, key) when is_reference(tag) do
    request_registry =
      RequestRegistry.update(state.request_registry, tag, &Map.put(&1, :connection_key, key))

    %{state | request_registry: request_registry}
  end

  def put_waiter_connection_key(state, _waiter, _key), do: state

  @spec pending_endpoint_key(State.t(), reference()) :: {:ok, term()} | :error
  def pending_endpoint_key(state, tag) when is_reference(tag) do
    case RequestRegistry.get(state.request_registry, tag) do
      %{conn: connection} when is_pid(connection) ->
        ConnectionPool.endpoint_key(state.connection_pool, connection)

      _missing ->
        :error
    end
  end
end
