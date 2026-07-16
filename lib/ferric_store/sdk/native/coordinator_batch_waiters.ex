defmodule FerricStore.SDK.Native.CoordinatorBatchWaiters do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchScheduler, ConnectionPool}

  def resume_capacity(state, limit, advance_connections)
      when is_integer(limit) and limit > 0 and is_function(advance_connections, 2),
      do: resume_capacity_loop(state, limit, advance_connections)

  def resume_endpoint(state, endpoint_key, limit, advance_connections, advance_batch)
      when is_integer(limit) and limit > 0 and is_function(advance_connections, 2) and
             is_function(advance_batch, 2) do
    waiting_before = BatchScheduler.endpoint_waiting_size(state.batch_scheduler, endpoint_key)

    {batch_ids, scheduler} =
      BatchScheduler.take_endpoint_waiters(state.batch_scheduler, endpoint_key, limit)

    state =
      Enum.reduce(batch_ids, %{state | batch_scheduler: scheduler}, fn batch_id, state ->
        resume_waiting(state, batch_id, advance_connections, advance_batch)
      end)

    maybe_continue_endpoint(state, endpoint_key, waiting_before)
  end

  defp resume_capacity_loop(state, 0, _advance_connections) do
    if BatchScheduler.waiting_size(state.batch_scheduler) > 0 and
         not ConnectionPool.full?(state.connection_pool) do
      send(self(), :resume_waiting_batch_connections)
    end

    state
  end

  defp resume_capacity_loop(state, remaining, advance_connections) do
    if ConnectionPool.full?(state.connection_pool) do
      state
    else
      case BatchScheduler.pop_connection_waiter(state.batch_scheduler) do
        {{:value, batch_id}, scheduler} ->
          state
          |> Map.put(:batch_scheduler, scheduler)
          |> advance_connections.(batch_id)
          |> resume_capacity_loop(remaining - 1, advance_connections)

        {:empty, scheduler} ->
          %{state | batch_scheduler: scheduler}
      end
    end
  end

  defp resume_waiting(state, batch_id, advance_connections, advance_batch) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :connecting} -> advance_connections.(state, batch_id)
      %{phase: :running} -> advance_batch.(state, batch_id)
      _batch_or_nil -> state
    end
  end

  defp maybe_continue_endpoint(state, endpoint_key, waiting_before) do
    waiting_after = BatchScheduler.endpoint_waiting_size(state.batch_scheduler, endpoint_key)

    if waiting_after > 0 and waiting_after < waiting_before,
      do: send(self(), {:resume_waiting_batch_connections, endpoint_key})

    state
  end
end
