defmodule FerricStore.SDK.Native.CoordinatorSubmissionRuntime do
  @moduledoc false

  alias FerricStore.{RequestContext, RequestLimits}

  alias FerricStore.SDK.Native.{Admission, CoordinatorRequest, Topology, TopologyRuntime}

  @spec control(map(), GenServer.from(), non_neg_integer(), term(), map(), map()) :: tuple()
  def control(state, from, opcode, payload, context, callbacks) do
    case RequestLimits.admit(context.batch_item_count, state.limits.batch_items) do
      :ok -> control_admitted(state, from, opcode, payload, context, callbacks)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @spec routed(map(), GenServer.from(), non_neg_integer(), term(), term(), map(), map()) ::
          tuple()
  def routed(state, from, opcode, key, payload, context, callbacks) do
    case RequestLimits.admit(context.batch_item_count, state.limits.batch_items) do
      :ok -> routed_admitted(state, from, opcode, key, payload, context, callbacks)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  defp control_admitted(state, from, opcode, payload, context, callbacks) do
    request = CoordinatorRequest.control(from, opcode, payload, context)

    if Admission.full?(state) do
      {:reply, {:error, :client_backpressure}, state}
    else
      {endpoint, connection_key} = control_target(state, context)

      lane_id =
        RequestContext.option(context, :lane_id, CoordinatorRequest.default_lane_id(opcode))

      callbacks.queue.(state, endpoint, lane_id, request, connection_key)
    end
  end

  defp routed_admitted(state, from, opcode, key, payload, context, callbacks) do
    request = CoordinatorRequest.routed(from, opcode, key, payload, context)

    if Admission.full?(state) do
      {:reply, {:error, :client_backpressure}, state}
    else
      route(state, request, callbacks)
    end
  end

  defp route(state, request, callbacks) do
    case Topology.route_key(TopologyRuntime.current(state), request.key) do
      {:ok, route} ->
        callbacks.queue.(state, route.endpoint, route.lane_id, request, route.endpoint_key)

      {:error, reason} ->
        callbacks.retry.(state, request, reason)
    end
  end

  defp control_target(state, context) do
    case RequestContext.option(context, :endpoint) do
      nil ->
        endpoint = TopologyRuntime.control_endpoint(state)
        {endpoint, Topology.endpoint_key(endpoint)}

      endpoint ->
        {endpoint, nil}
    end
  end
end
