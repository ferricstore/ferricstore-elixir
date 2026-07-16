defmodule FerricStore.SDK.Native.KVBatchPreparer do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.MSetPair
  alias FerricStore.SDK.Native.{BatchPreparer, BatchRouter, KVPayloadPreparer, Topology}

  @type operation :: :del | :mget | :mset

  @spec prepare(Topology.t(), operation(), list() | map(), RequestContext.t()) ::
          {:ok, [map()]} | {:error, term()}
  def prepare(%Topology{} = topology, operation, items, %RequestContext{} = context)
      when operation in [:del, :mget, :mset] and
             (is_list(items) or (operation == :mset and is_map(items))) do
    {item_router, payload_builder} = preparation_callbacks(operation)
    group_preparer = &KVPayloadPreparer.prepare(&1, operation, context)

    BatchPreparer.prepare_compact(
      topology,
      items,
      item_router,
      payload_builder,
      group_preparer,
      context
    )
  end

  @spec callbacks(operation()) :: {(term() -> term()), (list() -> map())}
  def callbacks(operation) when operation in [:del, :mget],
    do: {&binary_key/1, &%{"keys" => &1}}

  def callbacks(:mset), do: {&mset_key/1, &mset_payload/1}

  @doc false
  @spec preparation_callbacks(operation()) :: {BatchRouter.item_router(), (list() -> map())}
  def preparation_callbacks(operation) when operation in [:del, :mget],
    do: {&compact_key/1, &%{"keys" => &1}}

  def preparation_callbacks(:mset), do: {&compact_mset_pair/1, &%{"pairs" => &1}}

  defp binary_key(key) when is_binary(key), do: key
  defp binary_key(key), do: {:error, {:invalid_route_key, key}}

  defp compact_key(key) when is_binary(key), do: {:ok, key, key}
  defp compact_key(key), do: {:error, {:invalid_route_key, key}}

  defp compact_mset_pair({key, value}) when is_binary(key) and is_binary(value),
    do: {:ok, key, {key, value}, :slot}

  defp compact_mset_pair(pair) do
    with {:ok, {key, value}} <- MSetPair.normalize(pair) do
      {:ok, key, {key, value}, :slot}
    end
  end

  defp mset_key({key, value}) when is_binary(key) and is_binary(value),
    do: {:group_by, key, :slot}

  defp mset_key(pair) do
    with {:ok, {key, _value}} <- MSetPair.normalize(pair) do
      {:group_by, key, :slot}
    end
  end

  defp mset_payload(pairs), do: %{"pairs" => Enum.map(pairs, &mset_pair/1)}

  defp mset_pair({key, value}), do: %{"key" => key, "value" => value}
  defp mset_pair([key, value]), do: %{"key" => key, "value" => value}

  defp mset_pair(%{"key" => _key, "value" => _value} = pair) when map_size(pair) == 2,
    do: pair

  defp mset_pair(%{key: key, value: value} = pair) when map_size(pair) == 2,
    do: %{"key" => key, "value" => value}
end
