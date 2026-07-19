defmodule FerricStore.SDK.Native.BatchWireExecution do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Admission,
    BatchExecution,
    BatchPolicy,
    BatchScheduler,
    Connection,
    ConnectionPool,
    CoordinatorRequest,
    CoordinatorTimers,
    RequestRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @default_timeout 5_000

  @spec advance(State.t(), reference()) :: BatchExecution.action()
  def advance(%State{} = state, batch_id) when is_reference(batch_id) do
    case state |> clear_wire_wait(batch_id) |> fill_slots(batch_id) do
      {:timeout, state} ->
        {:timeout, state}

      {:ok, state} ->
        case BatchScheduler.get(state.batch_scheduler, batch_id) do
          %{phase: :running, queued: [], inflight: 0} -> {:finish, state}
          _batch_or_nil -> {:continue, state}
        end
    end
  end

  @spec handle_result(State.t(), map(), term()) :: BatchExecution.action()
  def handle_result(%State{} = state, request, result) do
    case BatchScheduler.get(state.batch_scheduler, request.batch_id) do
      nil ->
        {:continue, state}

      batch ->
        batch = record_result(batch, request, result)
        state = put_batch(state, batch)
        advance(state, batch.id)
    end
  end

  defp fill_slots(state, batch_id) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :running, queued: [group | rest]} = batch ->
        available =
          Admission.wire_slots(
            state.limits.pending_requests,
            RequestRegistry.size(state.request_registry)
          )

        cond do
          CoordinatorTimers.expired?(batch.opts) ->
            {:timeout, state}

          batch.inflight < batch.max_concurrency and available > 0 ->
            lease_group(state, batch, group, rest)

          batch.inflight == 0 and available == 0 ->
            {:ok, wait_for_wire_slot(state, batch.id)}

          true ->
            {:ok, state}
        end

      _batch_or_nil ->
        {:ok, state}
    end
  end

  defp lease_group(state, batch, group, rest) do
    route = group.route

    case ConnectionPool.reserve(state.connection_pool, route.endpoint_key, route.lane_id) do
      {:ok, conn, pool} ->
        state = %{state | connection_pool: pool}
        state = put_batch(state, %{batch | queued: rest})
        state = submit_group(state, batch.id, group, conn)
        fill_slots(state, batch.id)

      {:error, :capacity, pool} ->
        scheduler =
          BatchScheduler.wait_for_connection(
            state.batch_scheduler,
            batch.id,
            route.endpoint_key
          )

        {:ok, %{state | connection_pool: pool, batch_scheduler: scheduler}}

      {:error, :missing, pool} ->
        failure = BatchPolicy.group_failure(group, :connection_closed)
        batch = %{batch | queued: rest, failures: [failure | batch.failures]}

        state
        |> Map.put(:connection_pool, pool)
        |> put_batch(batch)
        |> fill_slots(batch.id)
    end
  end

  defp submit_group(state, batch_id, group, conn) do
    batch = BatchScheduler.fetch!(state.batch_scheduler, batch_id)
    tag = make_ref()
    timeout = CoordinatorTimers.connection_timeout(batch.opts, @default_timeout)

    Connection.acknowledged_async_request(
      conn,
      self(),
      tag,
      batch.opcode,
      group.payload,
      group.route.lane_id,
      timeout
    )

    timer = CoordinatorTimers.pending_request_timer(tag, batch.opts)

    request =
      batch_id
      |> CoordinatorRequest.batch_group(Map.put(group, :conn, conn), tag, timer, batch.opts)
      |> Map.put(:skip_connection_mark, true)

    state
    |> State.put_pending_request(tag, request)
    |> put_batch(%{
      batch
      | inflight: batch.inflight + 1,
        request_tags: MapSet.put(batch.request_tags, tag)
    })
  end

  defp record_result(batch, request, result) do
    batch = %{
      batch
      | inflight: max(batch.inflight - 1, 0),
        request_tags: MapSet.delete(batch.request_tags, request.tag)
    }

    case result do
      {:ok, value} ->
        success = request.group |> Map.take([:indexes]) |> Map.put(:value, value)
        %{batch | successes: [success | batch.successes]}

      {:error, reason} ->
        failure = BatchPolicy.group_failure(request.group, reason)
        %{batch | failures: [failure | batch.failures]}
    end
  end

  defp wait_for_wire_slot(state, batch_id) do
    scheduler = BatchScheduler.wait_for_wire_slot(state.batch_scheduler, batch_id)
    %{state | batch_scheduler: scheduler}
  end

  defp clear_wire_wait(state, batch_id) do
    scheduler = BatchScheduler.clear_wire_wait(state.batch_scheduler, batch_id)
    %{state | batch_scheduler: scheduler}
  end

  defp put_batch(state, batch),
    do: %{state | batch_scheduler: BatchScheduler.put(state.batch_scheduler, batch)}
end
