defmodule FerricStore.SDK.Native.CoordinatorBatchOrchestrator do
  @moduledoc false

  alias FerricStore.SDK.Native.{CoordinatorBatchRuntime, CoordinatorBatchWireRuntime}

  @resume_limit 64

  @type callbacks :: %{
          required(:cancel_refresh) => (map(), term() -> map()),
          required(:ensure_connection) => (map(), map(), term(), non_neg_integer(), term() ->
                                             map()),
          required(:pump_warm) => (map() -> map()),
          required(:start_refresh) => (map(), term() -> {:noreply, map()})
        }

  def finish_preparation(state, batch, result, callbacks) do
    state
    |> CoordinatorBatchRuntime.finish_preparation(
      batch,
      result,
      callbacks.ensure_connection
    )
    |> finish_transition(callbacks)
  end

  def start(state, batch, groups, callbacks) do
    state
    |> CoordinatorBatchRuntime.start(batch, groups, callbacks.ensure_connection)
    |> finish_transition(callbacks)
  end

  def resume_connection(state, batch_id, group_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.resume_connection(
      batch_id,
      group_id,
      callbacks.ensure_connection
    )
    |> finish_transition(callbacks)
  end

  def fail_connection(state, batch_id, group_id, reason, callbacks) do
    state
    |> CoordinatorBatchRuntime.fail_connection(
      batch_id,
      group_id,
      reason,
      callbacks.ensure_connection
    )
    |> finish_transition(callbacks)
  end

  def advance(state, batch_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.advance(batch_id)
    |> finish_transition(callbacks)
  end

  def handle_group_result(state, request, result, callbacks) do
    state
    |> CoordinatorBatchRuntime.handle_group_result(request, result)
    |> finish_transition(callbacks)
  end

  def resume_retry(state, batch_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.resume_retry(batch_id)
    |> finish_transition(callbacks)
  end

  def timeout(state, batch_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.timeout(batch_id)
    |> finish_transition(callbacks)
  end

  def abandon(state, batch_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.abandon(batch_id)
    |> finish_transition(callbacks)
  end

  def fail_preparer(state, batch_id, reason, callbacks) do
    state
    |> CoordinatorBatchRuntime.fail_preparer(batch_id, reason)
    |> finish_transition(callbacks)
  end

  def resume_capacity(result, endpoint_key, callbacks) do
    CoordinatorBatchWireRuntime.resume_capacity(
      result,
      endpoint_key,
      @resume_limit,
      &advance(&1, &2, callbacks),
      &resume_waiting_endpoint(&1, &2, callbacks)
    )
  end

  def resume_waiting_connections(state, callbacks) do
    CoordinatorBatchRuntime.resume_waiting_connections(
      state,
      @resume_limit,
      &advance_connections(&1, &2, callbacks)
    )
  end

  def resume_waiting_endpoint(state, endpoint_key, callbacks) do
    CoordinatorBatchRuntime.resume_waiting_endpoint(
      state,
      endpoint_key,
      @resume_limit,
      &advance_connections(&1, &2, callbacks),
      &advance(&1, &2, callbacks)
    )
  end

  def resume_waiting_wire_slots(state, callbacks) do
    CoordinatorBatchWireRuntime.resume(
      state,
      @resume_limit,
      &advance(&1, &2, callbacks)
    )
  end

  defp advance_connections(state, batch_id, callbacks) do
    state
    |> CoordinatorBatchRuntime.advance_connections(batch_id, callbacks.ensure_connection)
    |> finish_transition(callbacks)
  end

  defp finish_transition({:ok, state}, _callbacks), do: state

  defp finish_transition({:refresh, state, waiter}, callbacks) do
    {:noreply, state} = callbacks.start_refresh.(state, waiter)
    state
  end

  defp finish_transition({:cleanup, state, waiter, resume?}, callbacks) do
    state = resume_waiting_wire_slots(state, callbacks)

    state =
      if resume? do
        state
        |> resume_waiting_connections(callbacks)
        |> callbacks.pump_warm.()
      else
        state
      end

    callbacks.cancel_refresh.(state, waiter)
  end
end
