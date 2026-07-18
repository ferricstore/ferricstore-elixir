defmodule FerricStore.SDK.Native.CoordinatorBatchOrchestration do
  @moduledoc false

  alias FerricStore.SDK.Native.CoordinatorBatchOrchestrator

  def finish_preparation(state, batch, result, callbacks) do
    state =
      CoordinatorBatchOrchestrator.finish_preparation(
        state,
        batch,
        result,
        callbacks
      )

    {:noreply, state}
  end

  def start(state, batch, groups, callbacks),
    do:
      CoordinatorBatchOrchestrator.start(
        state,
        batch,
        groups,
        callbacks
      )

  def resume_connection(state, batch_id, group_id, _conn, callbacks),
    do:
      CoordinatorBatchOrchestrator.resume_connection(
        state,
        batch_id,
        group_id,
        callbacks
      )

  def fail_connection(state, batch_id, group_id, reason, callbacks),
    do:
      CoordinatorBatchOrchestrator.fail_connection(
        state,
        batch_id,
        group_id,
        reason,
        callbacks
      )

  def handle_group_result(state, request, result, callbacks) do
    state =
      CoordinatorBatchOrchestrator.handle_group_result(
        state,
        request,
        result,
        callbacks
      )

    {:noreply, state}
  end

  def resume_retry(state, batch_id, callbacks),
    do: CoordinatorBatchOrchestrator.resume_retry(state, batch_id, callbacks)

  def timeout(state, batch_id, callbacks),
    do:
      CoordinatorBatchOrchestrator.timeout(
        state,
        batch_id,
        callbacks
      )

  def abandon(state, batch_id, callbacks),
    do:
      CoordinatorBatchOrchestrator.abandon(
        state,
        batch_id,
        callbacks
      )

  def fail_preparer(state, batch_id, reason, callbacks),
    do:
      CoordinatorBatchOrchestrator.fail_preparer(
        state,
        batch_id,
        reason,
        callbacks
      )

  def resume_capacity(result, endpoint_key, callbacks),
    do:
      CoordinatorBatchOrchestrator.resume_capacity(
        result,
        endpoint_key,
        callbacks
      )

  def resume_waiting_connections(state, callbacks),
    do:
      CoordinatorBatchOrchestrator.resume_waiting_connections(
        state,
        callbacks
      )

  def resume_waiting_endpoint(state, endpoint_key, callbacks),
    do:
      CoordinatorBatchOrchestrator.resume_waiting_endpoint(
        state,
        endpoint_key,
        callbacks
      )

  def resume_waiting_wire_slots(state, callbacks),
    do:
      CoordinatorBatchOrchestrator.resume_waiting_wire_slots(
        state,
        callbacks
      )
end
