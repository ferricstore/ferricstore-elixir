defmodule FerricStore.SDK.Native.Topology.RangePreparer do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.SDK.Native.Topology.EndpointResolver

  @max_ranges 1_024

  @spec prepare(list(), pos_integer(), map()) ::
          {:ok, list(), map(), MapSet.t()} | {:error, term()}
  def prepare(ranges, shard_count, default_endpoint) do
    with {:ok, _range_count} <- admit(ranges) do
      prepare(ranges, shard_count, default_endpoint, [], %{}, MapSet.new(), %{})
    end
  end

  defp admit(ranges) do
    case BoundedList.count(ranges, @max_ranges) do
      {:ok, count} -> {:ok, count}
      {:error, {:limit_exceeded, _observed}} -> topology_too_large()
      {:error, :improper_list} -> {:error, :invalid_shards_payload}
    end
  end

  defp topology_too_large,
    do: {:error, {:topology_too_large, %{field: "ranges", limit: @max_ranges}}}

  defp prepare([], _shard_count, _default_endpoint, ranges, endpoints, shards, _cache),
    do: {:ok, ranges, endpoints, shards}

  defp prepare(
         [range | ranges],
         shard_count,
         default_endpoint,
         prepared,
         endpoints,
         shards,
         endpoint_cache
       ) do
    case prepare_range(range, shard_count, default_endpoint, endpoint_cache) do
      {:ok, item, endpoint_cache} ->
        prepare(
          ranges,
          shard_count,
          default_endpoint,
          [item | prepared],
          Map.put(endpoints, item.endpoint_key, item.endpoint),
          MapSet.put(shards, item.shard),
          endpoint_cache
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_range(
         %{"hint" => "leader_unknown"} = range,
         _shard_count,
         _default_endpoint,
         _endpoint_cache
       ) do
    case fetch_int(range, "shard") do
      {:ok, shard} -> {:error, {:leader_unknown, shard}}
      {:error, _reason} -> {:error, {:leader_unknown, range}}
    end
  end

  defp prepare_range(range, shard_count, default_endpoint, endpoint_cache)
       when is_map(range) do
    with {:ok, first} <- fetch_int(range, "first_slot"),
         {:ok, last} <- fetch_int(range, "last_slot"),
         {:ok, shard} <- fetch_int(range, "shard"),
         {:ok, lane_id} <- fetch_int(range, "lane_id"),
         {:ok, endpoint, endpoint_key, endpoint_cache} <-
           EndpointResolver.resolve(range, default_endpoint, endpoint_cache),
         true <- first >= 0 and first < @max_ranges,
         true <- last >= first and last < @max_ranges,
         true <- shard >= 0 and shard < shard_count,
         true <- lane_id >= 1 and lane_id <= 0xFFFF_FFFF do
      route = %{
        shard: shard,
        lane_id: lane_id,
        endpoint_key: endpoint_key,
        endpoint: endpoint,
        leader_node: endpoint.node
      }

      {:ok,
       %{
         first: first,
         last: last,
         shard: shard,
         route: route,
         endpoint_key: endpoint_key,
         endpoint: endpoint,
         original: range
       }, endpoint_cache}
    else
      _invalid -> {:error, {:invalid_range, range}}
    end
  end

  defp prepare_range(range, _shard_count, _default_endpoint, _endpoint_cache),
    do: {:error, {:invalid_range, range}}

  defp fetch_int(map, key) do
    case map[key] do
      value when is_integer(value) -> {:ok, value}
      _invalid -> {:error, key}
    end
  end
end
