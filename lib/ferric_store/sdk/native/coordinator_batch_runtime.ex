defmodule FerricStore.SDK.Native.CoordinatorBatchRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchExecution,
    BatchPreflight,
    BatchScheduler,
    CoordinatorBatchCancellation,
    CoordinatorBatchCompletion,
    CoordinatorBatchWaiters,
    CoordinatorTimers,
    RetryPolicy,
    RetryScheduler
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type transition ::
          {:ok, State.t()}
          | {:refresh, State.t(), {:batch_retry, reference()}}
          | {:cleanup, State.t(), {:batch_retry, reference()}, boolean()}

  @type ensure_connection :: BatchPreflight.ensure_connection()

  @spec finish_preparation(
          State.t(),
          map(),
          {:ok, [map()]} | {:error, term()},
          ensure_connection()
        ) :: transition()
  def finish_preparation(state, batch, result, ensure_connection) do
    if CoordinatorTimers.expired?(batch.opts),
      do: timeout(state, batch.id),
      else: finish_active_preparation(state, batch, result, ensure_connection)
  end

  defp finish_active_preparation(state, batch, {:ok, groups}, ensure_connection),
    do: start(state, batch, groups, ensure_connection)

  defp finish_active_preparation(state, batch, {:error, reason}, _ensure_connection) do
    if batch.attempt == 0 and RetryPolicy.retryable?(reason, batch.opcode, batch.opts) do
      batch = %{batch | attempt: 1, original_reason: reason, phase: :refreshing}

      case RetryScheduler.batch(batch.id, reason) do
        :ready -> {:refresh, put_batch(state, batch), {:batch_retry, batch.id}}
        :waiting -> {:ok, put_batch(state, batch)}
      end
    else
      {_batch, batch_scheduler} = BatchScheduler.pop(state.batch_scheduler, batch.id)
      state = %{state | batch_scheduler: batch_scheduler}
      {:ok, CoordinatorBatchCompletion.reply_error(state, batch, reason)}
    end
  end

  @spec start(State.t(), map(), [map()], ensure_connection()) :: transition()
  def start(state, batch, groups, ensure_connection) do
    state
    |> BatchPreflight.start(batch, groups, ensure_connection)
    |> finish_preflight()
  end

  @spec advance_connections(State.t(), reference(), ensure_connection()) :: transition()
  def advance_connections(state, batch_id, ensure_connection) do
    state
    |> BatchPreflight.advance(batch_id, ensure_connection)
    |> finish_preflight()
  end

  @spec resume_connection(State.t(), reference(), non_neg_integer(), ensure_connection()) ::
          transition()
  def resume_connection(state, batch_id, group_id, ensure_connection) do
    state
    |> BatchPreflight.resume(batch_id, group_id, ensure_connection)
    |> finish_preflight()
  end

  @spec fail_connection(State.t(), reference(), non_neg_integer(), term(), ensure_connection()) ::
          transition()
  def fail_connection(state, batch_id, group_id, reason, ensure_connection) do
    state
    |> BatchPreflight.fail(batch_id, group_id, reason, ensure_connection)
    |> finish_preflight()
  end

  @spec advance(State.t(), reference()) :: transition()
  def advance(state, batch_id) do
    state
    |> BatchExecution.advance(batch_id)
    |> finish_action(batch_id)
  end

  @spec handle_group_result(State.t(), map(), term()) :: transition()
  def handle_group_result(state, request, result) do
    state
    |> BatchExecution.handle_result(request, result)
    |> finish_action(request.batch_id)
  end

  def timeout(state, batch_id),
    do: CoordinatorBatchCancellation.cancel(state, batch_id, {:error, :timeout})

  def abandon(state, batch_id), do: CoordinatorBatchCancellation.cancel(state, batch_id, nil)

  def fail_preparer(state, batch_id, reason),
    do: CoordinatorBatchCompletion.fail_preparer(state, batch_id, reason)

  def fail_retry(state, batch_id, reason),
    do: CoordinatorBatchCompletion.fail_retry(state, batch_id, reason)

  def resume_retry(state, batch_id) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      nil ->
        {:ok, state}

      batch ->
        if CoordinatorTimers.expired?(batch.opts),
          do: timeout(state, batch_id),
          else: {:refresh, state, {:batch_retry, batch_id}}
    end
  end

  def resume_waiting_connections(state, limit, advance_connections),
    do: CoordinatorBatchWaiters.resume_capacity(state, limit, advance_connections)

  def resume_waiting_endpoint(
        state,
        endpoint_key,
        limit,
        advance_connections,
        advance_batch
      ),
      do:
        CoordinatorBatchWaiters.resume_endpoint(
          state,
          endpoint_key,
          limit,
          advance_connections,
          advance_batch
        )

  defp finish_preflight({:continue, state}), do: {:ok, state}
  defp finish_preflight({:run, state, batch_id}), do: advance(state, batch_id)

  defp finish_preflight({:finish, state, batch_id}),
    do: CoordinatorBatchCompletion.finish(state, batch_id)

  defp finish_preflight({:timeout, state, batch_id}), do: timeout(state, batch_id)

  defp finish_action({:continue, state}, _batch_id), do: {:ok, state}

  defp finish_action({:finish, state}, batch_id),
    do: CoordinatorBatchCompletion.finish(state, batch_id)

  defp finish_action({:timeout, state}, batch_id), do: timeout(state, batch_id)

  defp put_batch(state, batch),
    do: %{state | batch_scheduler: BatchScheduler.put(state.batch_scheduler, batch)}
end
