defmodule FerricStore.SDK.Native.BatchGroupedRouter do
  @moduledoc false

  alias FerricStore.SDK.Native.Topology

  @spec put(Topology.t(), binary(), term(), non_neg_integer(), map(), term()) ::
          {:cont, map()} | {:halt, {:error, term()}}
  def put(topology, key, item, index, groups, grouping) do
    case Topology.route_key(topology, key) do
      {:ok, route} -> put_routed(route, item, index, groups, grouping)
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp put_routed(route, item, index, groups, grouping) do
    group_key = group_key(route, grouping)
    group = Map.get(groups, group_key, %{route: route, items: [], indexes: []})

    {:cont,
     Map.put(groups, group_key, %{
       group
       | items: [item | group.items],
         indexes: [index | group.indexes]
     })}
  end

  defp group_key(route, :slot), do: {route.endpoint_key, route.lane_id, {:slot, route.slot}}
  defp group_key(route, grouping), do: {route.endpoint_key, route.lane_id, grouping}
end
