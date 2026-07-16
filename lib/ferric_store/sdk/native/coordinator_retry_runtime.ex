defmodule FerricStore.SDK.Native.CoordinatorRetryRuntime do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    CoordinatorReply,
    CoordinatorRequestRuntime,
    CoordinatorRetryTarget,
    CoordinatorTimers,
    RequestRegistry,
    RetryPolicy,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec completed(State.t(), map(), term(), map()) :: {:noreply, State.t()}
  def completed(state, request, reason, callbacks) do
    if request.attempt == 0 and RetryPolicy.retryable?(reason, request.opcode, request.opts) do
      retry(state, request, reason, callbacks)
    else
      result =
        if request.attempt > 0,
          do: {:error, {:retry_failed, request.original_reason, reason}},
          else: {:error, reason}

      callbacks.reply_completed.(state, request, result)
    end
  end

  @spec initial(State.t(), map(), term(), map()) ::
          {:noreply, State.t()} | {:reply, {:error, term()}, State.t()}
  def initial(state, request, reason, callbacks) do
    if RetryPolicy.retryable?(reason, request.opcode, request.opts),
      do: retry(state, request, reason, callbacks),
      else: {:reply, {:error, reason}, state}
  end

  @spec dispatch_pending(State.t(), reference(), map()) :: State.t()
  def dispatch_pending(state, tag, callbacks) do
    dispatch_pending(state, tag, RequestRegistry.get(state.request_registry, tag), callbacks)
  end

  @spec fail_pending(State.t(), reference(), term(), map()) :: State.t()
  def fail_pending(state, tag, reason, callbacks) do
    case State.pop_pending_request(state, tag) do
      {nil, state} ->
        callbacks.resume_wire_slots.(state)

      {request, state} ->
        CoordinatorTimers.cancel(request.timer)
        result = {:error, {:retry_failed, request.original_reason, reason}}
        {:noreply, state} = reply_or_return(state, request, result, callbacks)
        callbacks.resume_wire_slots.(state)
    end
  end

  defp retry(state, request, original_reason, callbacks) do
    request = %{request | attempt: 1, original_reason: original_reason}
    lane_id = Map.get(request, :lane_id, callbacks.default_lane.(request.opcode))
    {tag, state} = CoordinatorRequestRuntime.register(state, request, lane_id)
    callbacks.start_refresh.(state, {:request_retry, tag})
  end

  defp dispatch_pending(state, tag, %{kind: :routed} = request, callbacks) do
    case Topology.route_key(TopologyRuntime.current(state), request.key) do
      {:ok, route} ->
        dispatch_to_endpoint(
          state,
          tag,
          route.endpoint,
          route.lane_id,
          route.endpoint_key,
          callbacks
        )

      {:error, reason} ->
        fail_registered(state, tag, reason, callbacks)
    end
  end

  defp dispatch_pending(state, tag, %{kind: :control} = request, callbacks) do
    {endpoint, connection_key} = CoordinatorRetryTarget.control(state, request.opts)

    lane_id =
      RequestContext.option(request.opts, :lane_id, callbacks.default_lane.(request.opcode))

    dispatch_to_endpoint(state, tag, endpoint, lane_id, connection_key, callbacks)
  end

  defp dispatch_pending(state, tag, %{kind: kind} = request, callbacks)
       when kind in [:event_subscribe, :event_unsubscribe],
       do: dispatch_event(state, tag, request, callbacks)

  defp dispatch_pending(state, _tag, nil, _callbacks), do: state

  defp dispatch_event(state, tag, request, callbacks) do
    connection = State.event_connection(state)

    if is_pid(connection) and Process.alive?(connection) do
      dispatch_registered(state, tag, connection, callbacks)
    else
      {endpoint, connection_key} = CoordinatorRetryTarget.control(state, request.opts)

      dispatch_to_endpoint(state, tag, endpoint, 0, connection_key, callbacks)
    end
  end

  defp dispatch_to_endpoint(state, tag, endpoint, lane_id, connection_key, callbacks) do
    request_registry =
      RequestRegistry.update!(state.request_registry, tag, &Map.put(&1, :lane_id, lane_id))

    state = %{state | request_registry: request_registry}

    case callbacks.ensure_connection.(state, endpoint, connection_key, tag) do
      {:ok, connection, next_state} ->
        dispatch_registered(next_state, tag, connection, callbacks)

      {:waiting, next_state} ->
        next_state

      {:error, reason, next_state} ->
        fail_registered(next_state, tag, reason, callbacks)
    end
  end

  defp dispatch_registered(state, tag, connection, callbacks) do
    {:noreply, state} =
      CoordinatorRequestRuntime.dispatch_registered(
        state,
        tag,
        connection,
        callbacks.request_runtime
      )

    state
  end

  defp fail_registered(state, tag, reason, callbacks) do
    {:noreply, state} =
      CoordinatorRequestRuntime.fail(state, tag, reason, callbacks.request_runtime)

    state
  end

  defp reply_or_return(state, %{kind: kind} = request, result, callbacks)
       when kind in [:event_subscribe, :event_unsubscribe],
       do: callbacks.reply_completed.(state, request, result)

  defp reply_or_return(state, request, result, _callbacks) do
    CoordinatorTimers.demonitor(Map.get(request, :caller_monitor))
    CoordinatorReply.reply(request.from, result)
    {:noreply, state}
  end
end
