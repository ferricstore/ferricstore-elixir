defmodule FerricStore.SDK.Native.BatchRequestCancellation do
  @moduledoc false

  alias FerricStore.SDK.Native.{Connection, ConnectionPool, CoordinatorTimers, RequestRegistry}
  alias FerricStore.SDK.Native.Coordinator.State

  @spec cancel(State.t(), map()) :: State.t()
  def cancel(%State{} = state, batch) do
    {request_registry, cancelled} =
      RequestRegistry.pop_many(state.request_registry, batch.request_tags)

    Enum.each(cancelled, &cancel_request/1)
    connection_pool = Enum.reduce(cancelled, state.connection_pool, &release_request/2)

    state
    |> Map.put(:request_registry, request_registry)
    |> Map.put(:connection_pool, connection_pool)
    |> State.adjust_batch_groups(-length(cancelled))
  end

  defp cancel_request({tag, request}) do
    CoordinatorTimers.cancel(request.timer)

    if is_pid(Map.get(request, :conn)) do
      Connection.cancel_async(request.conn, self(), tag)
    end
  end

  defp release_request({_tag, %{conn: conn} = request}, pool) when is_pid(conn),
    do: ConnectionPool.mark_idle(pool, conn, request_lane(request))

  defp release_request(_request, pool), do: pool

  defp request_lane(%{group: %{route: %{lane_id: lane_id}}}), do: lane_id
  defp request_lane(%{lane_id: lane_id}), do: lane_id
  defp request_lane(_request), do: nil
end
