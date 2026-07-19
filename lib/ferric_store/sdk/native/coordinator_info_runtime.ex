defmodule FerricStore.SDK.Native.CoordinatorInfoRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorBatchPreparationRuntime,
    CoordinatorConnectionCleanup,
    CoordinatorConnectionResponseRuntime,
    CoordinatorConnectionRuntime,
    CoordinatorConnectionStartCompletion,
    CoordinatorEventRuntime,
    CoordinatorLifecycleRuntime,
    CoordinatorPendingRequestTimeout,
    CoordinatorRetryInfo,
    CoordinatorRuntime,
    CoordinatorServerEventRuntime,
    CoordinatorTopologyRefreshRuntime,
    EventRestoration,
    KVPreparationCoordinator,
    TopologyRefreshCall,
    TopologyRefreshCompletions
  }

  @spec handle(term(), map()) :: {:noreply, map()} | {:stop, term(), map()}

  def handle({:ferricstore_connection_response, _, _, _, _} = response, state),
    do: CoordinatorConnectionResponseRuntime.handle(state, response)

  def handle({:ferricstore_connection_response, _, _, _} = response, state),
    do: CoordinatorConnectionResponseRuntime.handle(state, response)

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

  def handle({:pending_request_timeout, tag}, state),
    do: CoordinatorPendingRequestTimeout.handle(state, tag)

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

  def handle({:ferricstore_connection_started, starter, token, key, result}, state),
    do: CoordinatorConnectionStartCompletion.handle(state, starter, token, key, result)

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
