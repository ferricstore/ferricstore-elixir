defmodule FerricStore.SDK.Native.CoordinatorInfoRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionPool,
    CoordinatorBatchPreparationRuntime,
    CoordinatorConnectionCleanup,
    CoordinatorConnectionRuntime,
    CoordinatorEventRuntime,
    CoordinatorLifecycleRuntime,
    CoordinatorRetryInfo,
    CoordinatorRuntime,
    CoordinatorServerEventRuntime,
    CoordinatorTopologyRefreshRuntime,
    EventRestoration,
    KVPreparationCoordinator,
    TopologyRefreshCall,
    TopologyRefreshCompletions
  }

  @spec handle(term(), map()) :: {:noreply, map()}
  def handle({:ferricstore_connection_response, conn, tag, result}, state) do
    endpoint_key = ConnectionPool.endpoint_key(state.connection_pool, conn)

    state
    |> CoordinatorRuntime.handle_connection_response(conn, tag, result)
    |> CoordinatorRuntime.resume_batch_capacity(endpoint_key)
  end

  def handle({:batch_prepared, preparer, token, batch_id, result}, state) do
    CoordinatorBatchPreparationRuntime.complete(
      state,
      preparer,
      token,
      batch_id,
      result,
      &CoordinatorRuntime.finish_batch_preparation/3
    )
  end

  def handle({:retry_event_restore, token}, state) do
    {:noreply,
     EventRestoration.retry(
       state,
       token,
       &CoordinatorRuntime.ensure_connection_async/3,
       &CoordinatorRuntime.dispatch_connection/4
     )}
  end

  def handle({kind, id}, state)
      when kind in [:retry_request, :retry_batch] and is_reference(id),
      do: CoordinatorRetryInfo.handle(kind, id, state)

  def handle({:pending_request_timeout, tag}, state) do
    endpoint_key = CoordinatorConnectionRuntime.pending_endpoint_key(state, tag)

    result =
      case CoordinatorRuntime.pop_pending_request(state, tag) do
        {nil, state} ->
          {:noreply, state}

        {request, state} ->
          if is_pid(Map.get(request, :conn)) do
            Connection.cancel_async(request.conn, self(), tag)
          end

          state =
            state
            |> CoordinatorRuntime.remove_connection_waiter(
              Map.get(request, :connection_key),
              tag
            )
            |> CoordinatorRuntime.cancel_refresh_waiter({:request_retry, tag})

          CoordinatorRuntime.handle_pending_timeout(state, request)
      end

    CoordinatorRuntime.resume_batch_capacity(result, endpoint_key)
  end

  def handle({:event_queue_timeout, event_call_id}, state),
    do: CoordinatorEventRuntime.timeout_queued(state, event_call_id)

  def handle({:refresh_waiter_timeout, monitor, from}, state) when is_reference(monitor) do
    TopologyRefreshCall.timeout(
      state,
      monitor,
      from,
      &CoordinatorRuntime.cancel_refresh_waiter/2
    )
  end

  def handle({:batch_timeout, batch_id}, state),
    do: {:noreply, CoordinatorRuntime.timeout_batch(state, batch_id)}

  def handle(:resume_waiting_batch_connections, state),
    do: {:noreply, CoordinatorRuntime.resume_waiting_batch_connections(state)}

  def handle(:resume_waiting_batch_wire_slots, state),
    do: {:noreply, CoordinatorRuntime.resume_waiting_batch_wire_slots(state)}

  def handle({:resume_waiting_batch_connections, endpoint_key}, state),
    do: {:noreply, CoordinatorRuntime.resume_waiting_batch_endpoint(state, endpoint_key)}

  def handle(:resume_topology_refresh_waiters, state) do
    state =
      TopologyRefreshCompletions.resume(
        state,
        &CoordinatorRuntime.finish_topology_refresh_waiter/3
      )

    {:noreply, state}
  end

  def handle({:ferricstore_connection_started, starter, token, key, result}, state) do
    case ConnectionPool.pop_attempt(state.connection_pool, key) do
      {%{starter: ^starter, token: ^token} = attempt, pool} ->
        Process.demonitor(attempt.monitor, [:flush])

        state =
          state
          |> Map.put(:connection_pool, pool)
          |> CoordinatorRuntime.delete_lifecycle_monitor(
            attempt.monitor,
            {:connection_attempt, attempt.key}
          )

        CoordinatorRuntime.handle_connection_started(state, attempt, result)

      {attempt, pool} ->
        pool = if attempt, do: ConnectionPool.put_attempt(pool, key, attempt), else: pool
        state = %{state | connection_pool: pool}
        CoordinatorConnectionCleanup.discard_start(state, result)
    end
  end

  def handle({:ferricstore_topology_refreshed, refresher, token, result}, state) do
    case CoordinatorTopologyRefreshRuntime.operation(state) do
      %{refresher: ^refresher, token: ^token} = operation ->
        Process.demonitor(operation.monitor, [:flush])

        state =
          state
          |> CoordinatorRuntime.delete_lifecycle_monitor(
            operation.monitor,
            {:topology_refresh, operation.token}
          )
          |> CoordinatorTopologyRefreshRuntime.put_operation(nil)
          |> CoordinatorTopologyRefreshRuntime.release(operation)

        CoordinatorTopologyRefreshRuntime.finish(
          state,
          operation,
          result,
          CoordinatorRuntime.topology_refresh_callbacks()
        )

      _other ->
        CoordinatorConnectionCleanup.discard_refresh(state, result)
    end
  end

  def handle({:ferricstore_server_frame, conn, opcode, value}, state) do
    CoordinatorServerEventRuntime.handle(
      state,
      conn,
      opcode,
      value,
      CoordinatorRuntime.server_event_callbacks()
    )
  end

  def handle({:ferricstore_connection_capacity, conn, capacity}, state),
    do:
      CoordinatorConnectionRuntime.update_capacity(
        state,
        conn,
        capacity,
        &CoordinatorRuntime.resume_waiting_batch_endpoint/2
      )

  def handle({:preparation_reservation_timeout, reservation}, state)
      when is_reference(reservation),
      do: {:noreply, KVPreparationCoordinator.drop(state, reservation)}

  def handle({:DOWN, monitor, :process, process, reason}, state) do
    CoordinatorLifecycleRuntime.down(
      state,
      monitor,
      process,
      reason,
      CoordinatorRuntime.lifecycle_callbacks()
    )
  end

  def handle(_message, state), do: {:noreply, state}
end
