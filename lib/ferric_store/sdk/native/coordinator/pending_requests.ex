defmodule FerricStore.SDK.Native.Coordinator.PendingRequests do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionPool, RequestRegistry}
  alias FerricStore.SDK.Native.Coordinator.StateLifecycle

  def put(state, tag, request) when is_reference(tag) and is_map(request) do
    {_previous, state} = pop(state, tag)
    {request, connection_pool} = register_connection_load(state.connection_pool, request)

    state = %{
      state
      | request_registry: RequestRegistry.put(state.request_registry, tag, request),
        connection_pool: connection_pool
    }

    state = put_caller_monitor(state, request, tag)

    if Map.get(request, :kind) == :batch_group,
      do: StateLifecycle.adjust_batch_groups(state, 1),
      else: state
  end

  def pop(state, tag) when is_reference(tag) do
    case RequestRegistry.pop(state.request_registry, tag) do
      {nil, _registry} ->
        {nil, state}

      {request, request_registry} ->
        state = %{
          state
          | request_registry: request_registry,
            connection_pool: release_connection_load(state.connection_pool, request)
        }

        state = delete_caller_monitor(state, request, tag)

        state =
          if Map.get(request, :kind) == :batch_group,
            do: StateLifecycle.adjust_batch_groups(state, -1),
            else: state

        {request, state}
    end
  end

  defp put_caller_monitor(state, request, tag) do
    case Map.get(request, :caller_monitor) do
      monitor when is_reference(monitor) ->
        StateLifecycle.put_monitor(state, monitor, {:pending_request, tag})

      _missing ->
        state
    end
  end

  defp delete_caller_monitor(state, request, tag) do
    case Map.get(request, :caller_monitor) do
      monitor when is_reference(monitor) ->
        StateLifecycle.delete_monitor(state, monitor, {:pending_request, tag})

      _missing ->
        state
    end
  end

  defp register_connection_load(pool, %{skip_connection_mark: true} = request),
    do: {Map.delete(request, :skip_connection_mark), pool}

  defp register_connection_load(pool, %{conn: connection} = request) when is_pid(connection),
    do: {request, ConnectionPool.mark_busy(pool, connection, request_lane(request))}

  defp register_connection_load(pool, request), do: {request, pool}

  defp release_connection_load(pool, %{conn: connection} = request) when is_pid(connection),
    do: ConnectionPool.mark_idle(pool, connection, request_lane(request))

  defp release_connection_load(pool, _request), do: pool

  defp request_lane(%{lane_id: lane_id}) when is_integer(lane_id), do: lane_id

  defp request_lane(%{group: %{route: %{lane_id: lane_id}}}) when is_integer(lane_id),
    do: lane_id

  defp request_lane(_request), do: nil
end
