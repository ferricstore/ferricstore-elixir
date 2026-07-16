defmodule FerricStore.SDK.Native.BatchPreflight do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchExecution,
    BatchPreflightCompletion,
    BatchRetry,
    BatchScheduler,
    ConnectionPool,
    CoordinatorTimers
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type action ::
          {:continue, State.t()}
          | {:run, State.t(), reference()}
          | {:finish, State.t(), reference()}
          | {:timeout, State.t(), reference()}

  @type ensure_connection ::
          (State.t(), map(), term(), non_neg_integer(), term() ->
             {:ok, pid(), State.t()}
             | {:waiting, State.t()}
             | {:capacity, State.t()}
             | {:error, term(), State.t()})

  @spec start(State.t(), map(), [map()], ensure_connection()) :: action()
  def start(%State{} = state, batch, groups, ensure_connection)
      when is_list(groups) and is_function(ensure_connection, 5) do
    timer = batch.timer || CoordinatorTimers.batch_timer(batch.id, batch.opts)
    caller_monitor = batch.caller_monitor || Process.monitor(elem(batch.from, 0))
    batch = BatchRetry.release_inputs(batch)

    batch = %{
      batch
      | phase: :connecting,
        timer: timer,
        caller_monitor: caller_monitor,
        connections_remaining: length(groups),
        connections_inflight: 0,
        connecting_groups: %{},
        connection_queue: groups,
        ready_groups: [],
        queued: [],
        inflight: 0
    }

    state |> put_batch(batch) |> advance(batch.id, ensure_connection)
  end

  @spec advance(State.t(), reference(), ensure_connection()) :: action()
  def advance(%State{} = state, batch_id, ensure_connection)
      when is_reference(batch_id) and is_function(ensure_connection, 5) do
    state = clear_wait(state, batch_id)

    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :connecting} = batch ->
        if CoordinatorTimers.expired?(batch.opts) do
          {:timeout, state, batch_id}
        else
          state
          |> fill_slots(batch_id, ensure_connection)
          |> BatchPreflightCompletion.finish(batch_id)
        end

      _batch_or_nil ->
        {:continue, state}
    end
  end

  @spec resume(State.t(), reference(), non_neg_integer(), ensure_connection()) :: action()
  def resume(%State{} = state, batch_id, group_id, ensure_connection) do
    state =
      case BatchScheduler.get(state.batch_scheduler, batch_id) do
        %{phase: :connecting} = batch ->
          case Map.pop(batch.connecting_groups, group_id) do
            {nil, _connecting_groups} ->
              state

            {group, connecting_groups} ->
              put_batch(state, %{
                batch
                | connections_inflight: max(batch.connections_inflight - 1, 0),
                  connecting_groups: connecting_groups,
                  connection_queue: [group | batch.connection_queue]
              })
          end

        _batch_or_nil ->
          state
      end

    advance(state, batch_id, ensure_connection)
  end

  @spec fail(State.t(), reference(), non_neg_integer(), term(), ensure_connection()) :: action()
  def fail(%State{} = state, batch_id, group_id, reason, ensure_connection) do
    state
    |> record(batch_id, group_id, {:error, reason})
    |> advance(batch_id, ensure_connection)
  end

  defp fill_slots(state, batch_id, ensure_connection) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{
        phase: :connecting,
        connection_queue: [group | rest],
        connections_inflight: inflight,
        max_concurrency: max_concurrency
      } = batch
      when inflight < max_concurrency ->
        start_next(state, batch, group, rest, inflight, ensure_connection)

      _batch_or_nil ->
        state
    end
  end

  defp start_next(state, batch, group, rest, inflight, ensure_connection) do
    if ConnectionPool.slot_available?(state.connection_pool, group.route.endpoint_key) do
      group_id = group_id(group)

      batch = %{
        batch
        | connection_queue: rest,
          connections_inflight: inflight + 1,
          connecting_groups: Map.put(batch.connecting_groups, group_id, group)
      }

      state = put_batch(state, batch)
      waiter = {:batch, batch.id, group_id}

      state =
        case ensure_connection.(
               state,
               group.route.endpoint,
               group.route.endpoint_key,
               group.route.lane_id,
               waiter
             ) do
          {:ok, conn, state} -> record(state, batch.id, group_id, {:ok, conn})
          {:waiting, state} -> state
          {:capacity, state} -> record(state, batch.id, group_id, {:ok, nil})
          {:error, reason, state} -> record(state, batch.id, group_id, {:error, reason})
        end

      fill_slots(state, batch.id, ensure_connection)
    else
      wait_for_connection(state, batch.id, group.route.endpoint_key)
    end
  end

  defp record(state, batch_id, group_id, result) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :connecting} = batch ->
        case Map.pop(batch.connecting_groups, group_id) do
          {nil, _connecting_groups} ->
            state

          {group, connecting_groups} ->
            batch = BatchExecution.record_preflight(batch, group, connecting_groups, result)
            put_batch(state, batch)
        end

      _batch_or_nil ->
        state
    end
  end

  defp wait_for_connection(state, batch_id, endpoint_key) do
    scheduler =
      BatchScheduler.wait_for_connection(state.batch_scheduler, batch_id, endpoint_key)

    %{state | batch_scheduler: scheduler}
  end

  defp clear_wait(state, batch_id) do
    scheduler = BatchScheduler.clear_connection_wait(state.batch_scheduler, batch_id)
    %{state | batch_scheduler: scheduler}
  end

  defp group_id(%{indexes: [index | _indexes]}) when is_integer(index), do: index

  defp put_batch(state, batch),
    do: %{state | batch_scheduler: BatchScheduler.put(state.batch_scheduler, batch)}
end
