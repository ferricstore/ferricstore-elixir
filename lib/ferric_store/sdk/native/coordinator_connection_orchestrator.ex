defmodule FerricStore.SDK.Native.CoordinatorConnectionOrchestrator do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionLifecycle,
    CoordinatorConnectionAcquisition,
    CoordinatorConnectionRuntime,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec ensure(State.t(), map(), term()) :: CoordinatorConnectionAcquisition.ensure_result()
  def ensure(state, endpoint, waiter),
    do: CoordinatorConnectionAcquisition.ensure(state, endpoint, nil, waiter)

  @spec ensure(State.t(), map(), term(), term()) ::
          CoordinatorConnectionAcquisition.ensure_result()
  def ensure(state, endpoint, connection_key, waiter),
    do: CoordinatorConnectionAcquisition.ensure(state, endpoint, connection_key, waiter)

  @spec ensure_batch(State.t(), map(), term(), non_neg_integer(), term()) ::
          CoordinatorConnectionAcquisition.ensure_result()
  def ensure_batch(state, endpoint, connection_key, lane_id, waiter) do
    CoordinatorConnectionAcquisition.ensure_batch(
      state,
      endpoint,
      connection_key,
      lane_id,
      waiter
    )
  end

  def pump_warm(state), do: CoordinatorConnectionAcquisition.pump_warm(state)

  def retire(state, connection) do
    %{state | connection_pool: ConnectionLifecycle.retire(state.connection_pool, connection)}
  end

  def complete(state, attempt, result, callbacks) do
    CoordinatorConnectionAcquisition.complete(
      state,
      attempt,
      result,
      acquisition_callbacks(callbacks)
    )
  end

  def remove_waiter(state, key, tag, callbacks) do
    CoordinatorConnectionAcquisition.remove_waiter(
      state,
      key,
      tag,
      acquisition_callbacks(callbacks)
    )
  end

  def down(state, connection, reason, callbacks) do
    event_connection? = State.event_connection(state) == connection
    pool = ConnectionLifecycle.down(state.connection_pool, connection)

    state =
      %{state | connection_pool: pool}
      |> State.clear_event_connection(connection)
      |> CoordinatorConnectionRuntime.fail_requests(
        connection,
        reason,
        callbacks.handle_response
      )

    state =
      if event_connection?,
        do: callbacks.reconnect_event.(state),
        else: state

    {:noreply,
     state
     |> callbacks.resume_wire_slots.()
     |> callbacks.resume_waiting.()
     |> callbacks.pump_warm.()}
  end

  defp acquisition_callbacks(callbacks) do
    %{
      fail_waiter: &fail_waiter(&1, &2, &3, callbacks),
      maybe_restore: callbacks.start_event_restore,
      pump_warm: callbacks.pump_warm,
      resume_waiter: &resume_waiter(&1, &2, &3, callbacks),
      resume_waiting: callbacks.resume_waiting,
      resume_waiting_endpoint: callbacks.resume_waiting_endpoint
    }
  end

  defp resume_waiter(state, tag, connection, callbacks) when is_reference(tag) do
    {:noreply, state} = callbacks.dispatch_registered.(state, tag, connection)
    state
  end

  defp resume_waiter(state, {:batch, batch_id, group_id}, connection, callbacks),
    do: callbacks.resume_batch.(state, batch_id, group_id, connection)

  defp resume_waiter(state, {:warm_connection, key}, connection, _callbacks) do
    if Map.has_key?(TopologyRuntime.current(state).endpoints, key),
      do: state,
      else: retire(state, connection)
  end

  defp resume_waiter(state, {:event_reconnect, _key}, connection, callbacks),
    do: callbacks.start_event_restore.(state, connection)

  defp fail_waiter(state, tag, reason, callbacks) when is_reference(tag) do
    {:noreply, state} = callbacks.fail_registered.(state, tag, reason)
    state
  end

  defp fail_waiter(state, {:batch, batch_id, group_id}, reason, callbacks),
    do: callbacks.fail_batch.(state, batch_id, group_id, reason)

  defp fail_waiter(state, {:warm_connection, _key}, _reason, _callbacks), do: state

  defp fail_waiter(state, {:event_reconnect, _key}, reason, callbacks),
    do: callbacks.event_connection_failed.(state, reason)
end
