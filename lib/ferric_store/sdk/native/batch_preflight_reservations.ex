defmodule FerricStore.SDK.Native.BatchPreflightReservations do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchPolicy, ConnectionPool}
  alias FerricStore.SDK.Native.Coordinator.State

  @spec record(map(), map(), map(), {:ok, pid() | nil} | {:error, term()}) :: map()
  def record(batch, group, connecting_groups, {:ok, conn}) do
    group =
      if is_pid(conn),
        do: group |> Map.put(:conn, conn) |> Map.put(:preflight_reserved, true),
        else: group

    %{
      batch
      | connections_remaining: max(batch.connections_remaining - 1, 0),
        connections_inflight: max(batch.connections_inflight - 1, 0),
        connecting_groups: connecting_groups,
        ready_groups: [group | batch.ready_groups]
    }
  end

  def record(batch, group, connecting_groups, {:error, reason}) do
    %{
      batch
      | connections_remaining: max(batch.connections_remaining - 1, 0),
        connections_inflight: max(batch.connections_inflight - 1, 0),
        connecting_groups: connecting_groups,
        failures: [BatchPolicy.group_failure(group, reason) | batch.failures]
    }
  end

  @spec release(State.t(), map() | [map()]) :: State.t() | {State.t(), [map()]}
  def release(%State{} = state, %{ready_groups: groups}), do: elem(release(state, groups), 0)

  def release(%State{} = state, groups) when is_list(groups) do
    {groups, state} =
      Enum.map_reduce(groups, state, fn group, state ->
        state = release_group(state, group)
        {Map.drop(group, [:conn, :preflight_reserved]), state}
      end)

    {state, groups}
  end

  defp release_group(state, group) do
    case group do
      %{preflight_reserved: true, conn: conn, route: %{lane_id: lane_id}} when is_pid(conn) ->
        %{state | connection_pool: ConnectionPool.mark_idle(state.connection_pool, conn, lane_id)}

      _unreserved ->
        state
    end
  end
end
