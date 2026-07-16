defmodule FerricStore.SDK.Native.TopologyCandidates do
  @moduledoc false

  alias FerricStore.SDK.Native.Topology

  @spec select([map()], [map()], pos_integer()) :: [map()]
  def select(seeds, discovered, limit)
      when is_list(seeds) and is_list(discovered) and is_integer(limit) and limit > 0 do
    indexed_seeds = uniq_indexed(seeds)
    seed_keys = MapSet.new(indexed_seeds, &elem(&1, 0))

    discovered =
      discovered
      |> uniq_indexed()
      |> Enum.reject(fn {key, _endpoint} -> MapSet.member?(seed_keys, key) end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    seeds = Enum.map(indexed_seeds, &elem(&1, 1))

    seeds
    |> prioritize(discovered)
    |> Enum.take(limit)
  end

  @spec control([map()], [map()], map()) :: map()
  def control(discovered, seeds, default)
      when is_list(discovered) and is_list(seeds) and is_map(default) do
    case discovered do
      [_endpoint | _rest] -> Enum.min_by(discovered, &Topology.endpoint_key/1)
      [] -> List.first(seeds) || default
    end
  end

  defp prioritize([primary | fallback_seeds], [first_discovered | discovered]) do
    [primary, first_discovered] ++ fallback_seeds ++ discovered
  end

  defp prioritize(seeds, discovered), do: seeds ++ discovered

  defp uniq_indexed(endpoints) do
    {endpoints, _seen} =
      Enum.reduce(endpoints, {[], MapSet.new()}, fn endpoint, {unique, seen} ->
        key = Topology.endpoint_key(endpoint)

        if MapSet.member?(seen, key),
          do: {unique, seen},
          else: {[{key, endpoint} | unique], MapSet.put(seen, key)}
      end)

    Enum.reverse(endpoints)
  end
end
