defmodule FerricStore.SDK.Native.Topology.Builder do
  @moduledoc false

  alias FerricStore.SDK.Native.EndpointIdentity
  alias FerricStore.SDK.Native.Topology.RangePreparer
  alias FerricStore.Types

  @num_slots 1_024

  @spec build(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def build(payload, opts \\ [])

  def build(%{"ranges" => ranges} = payload, opts) when is_list(ranges) do
    default_endpoint = opts |> Keyword.get(:default_endpoint, %{}) |> EndpointIdentity.prepare()

    with {:ok, route_epoch} <- non_negative_int(payload, "route_epoch"),
         {:ok, shard_count} <- positive_int(payload, "shard_count"),
         true <- shard_count <= @num_slots,
         {:ok, prepared, endpoints, shards} <-
           RangePreparer.prepare(ranges, shard_count, default_endpoint),
         {:ok, slots} <- build_slots(prepared),
         :ok <- validate_shards(shards, shard_count) do
      {:ok,
       %{
         route_epoch: route_epoch,
         shard_count: shard_count,
         slots: slots,
         endpoints: endpoints,
         control_endpoint: control_endpoint(endpoints)
       }}
    else
      false -> {:error, :invalid_shard_count}
      {:error, _reason} = error -> error
    end
  end

  def build(%{ranges: ranges} = payload, opts) when is_list(ranges) do
    with {:ok, payload} <- Types.normalize_map_result(payload) do
      build(payload, opts)
    end
  end

  def build(_payload, _opts), do: {:error, :invalid_shards_payload}

  defp build_slots(prepared) do
    prepared
    |> Enum.sort_by(&{&1.first, &1.last})
    |> build_slot_chunks(0, [])
  end

  defp build_slot_chunks([], @num_slots, chunks) do
    slots = chunks |> Enum.reverse() |> :lists.append() |> List.to_tuple()
    {:ok, slots}
  end

  defp build_slot_chunks([], _next_slot, _chunks), do: {:error, :incomplete_topology}

  defp build_slot_chunks([%{first: first} | _ranges], next_slot, _chunks)
       when first > next_slot,
       do: {:error, :incomplete_topology}

  defp build_slot_chunks([%{first: first, original: range} | _ranges], next_slot, _chunks)
       when first < next_slot,
       do: {:error, {:invalid_range, range}}

  defp build_slot_chunks([range | ranges], next_slot, chunks) do
    slot_count = range.last - range.first + 1
    chunk = List.duplicate(range.route, slot_count)
    build_slot_chunks(ranges, next_slot + slot_count, [chunk | chunks])
  end

  defp validate_shards(shards, shard_count) do
    if MapSet.size(shards) == shard_count, do: :ok, else: {:error, :shard_count_mismatch}
  end

  defp control_endpoint(endpoints) do
    endpoints
    |> Enum.min_by(&elem(&1, 0))
    |> elem(1)
  end

  defp non_negative_int(map, key) do
    case map[key] do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _invalid -> {:error, {:invalid_topology_field, key}}
    end
  end

  defp positive_int(map, key) do
    case map[key] do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _invalid -> {:error, {:invalid_topology_field, key}}
    end
  end
end
