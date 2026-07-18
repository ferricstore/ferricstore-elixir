defmodule FerricStore.SDK.Native.CoordinatorRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorBatchOrchestration,
    CoordinatorCallRuntime,
    CoordinatorInitializer,
    CoordinatorLifecycleOrchestration,
    CoordinatorRuntimeCallbacks,
    CoordinatorShutdown,
    KVPreparationCoordinator
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec init(keyword()) :: {:ok, State.t()} | {:error, term()}
  def init(opts),
    do: CoordinatorInitializer.run(opts, &CoordinatorLifecycleOrchestration.initialize/1)

  @spec call(term(), GenServer.from(), State.t()) ::
          {:reply, term(), State.t()} | {:noreply, State.t()}
  def call(request, from, state),
    do: CoordinatorCallRuntime.handle(request, from, state, CoordinatorRuntimeCallbacks.call())

  @spec cast(term(), State.t()) :: {:noreply, State.t()}
  def cast({:release_kv_preparation, reservation, owner}, state)
      when is_reference(reservation) and is_pid(owner),
      do: {:noreply, KVPreparationCoordinator.release(state, owner, reservation)}

  @spec terminate(State.t()) :: :ok
  def terminate(state), do: CoordinatorShutdown.run(state, :client_closed)

  def lifecycle_callbacks, do: CoordinatorRuntimeCallbacks.lifecycle()
  def topology_refresh_callbacks, do: CoordinatorRuntimeCallbacks.topology_refresh()
  def server_event_callbacks, do: CoordinatorRuntimeCallbacks.server_event()

  def dispatch_connection(state, conn, lane_id, request),
    do: CoordinatorRuntimeCallbacks.dispatch_connection(state, conn, lane_id, request)

  def pop_pending_request(state, tag), do: State.pop_pending_request(state, tag)

  def delete_lifecycle_monitor(state, monitor, owner),
    do: State.delete_lifecycle_monitor(state, monitor, owner)

  def handle_connection_response(state, conn, tag, result),
    do: CoordinatorRuntimeCallbacks.handle_connection_response(state, conn, tag, result)

  def resume_batch_capacity(result, endpoint_key),
    do:
      CoordinatorBatchOrchestration.resume_capacity(
        result,
        endpoint_key,
        CoordinatorRuntimeCallbacks.batch()
      )

  def finish_batch_preparation(state, batch, result),
    do:
      CoordinatorBatchOrchestration.finish_preparation(
        state,
        batch,
        result,
        CoordinatorRuntimeCallbacks.batch()
      )

  def handle_pending_timeout(state, request),
    do: CoordinatorRuntimeCallbacks.handle_pending_timeout(state, request)

  def resume_request_retry(state, tag),
    do: CoordinatorRuntimeCallbacks.resume_retried_pending_request(state, tag)

  def resume_batch_retry(state, batch_id),
    do:
      CoordinatorBatchOrchestration.resume_retry(
        state,
        batch_id,
        CoordinatorRuntimeCallbacks.batch()
      )

  def timeout_batch(state, batch_id),
    do:
      CoordinatorBatchOrchestration.timeout(state, batch_id, CoordinatorRuntimeCallbacks.batch())

  def finish_topology_refresh_waiter(waiter, result, state),
    do:
      CoordinatorLifecycleOrchestration.finish_topology_refresh_waiter(
        waiter,
        result,
        state,
        CoordinatorRuntimeCallbacks.topology_refresh()
      )

  def cancel_refresh_waiter(state, key),
    do: CoordinatorLifecycleOrchestration.cancel_refresh_waiter(state, key)

  def ensure_connection_async(state, endpoint, waiter),
    do: CoordinatorLifecycleOrchestration.ensure_connection_async(state, endpoint, waiter)

  def handle_connection_started(state, attempt, result),
    do: CoordinatorRuntimeCallbacks.handle_connection_started(state, attempt, result)

  def remove_connection_waiter(state, key, tag),
    do: CoordinatorRuntimeCallbacks.remove_connection_waiter(state, key, tag)

  def resume_waiting_batch_connections(state),
    do: CoordinatorRuntimeCallbacks.resume_waiting_connections(state)

  def resume_waiting_batch_endpoint(state, endpoint_key),
    do: CoordinatorRuntimeCallbacks.resume_waiting_endpoint(state, endpoint_key)

  def resume_waiting_batch_wire_slots(state),
    do: CoordinatorRuntimeCallbacks.resume_waiting_wire_slots(state)
end
