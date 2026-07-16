defmodule FerricStore.SDK.Native.BatchRouter do
  @moduledoc false
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{BatchGroupedRouter, BatchItemRouter, BatchMapCollector, Topology}

  @deadline_check_interval 256

  @type routed_item ::
          {:ok, binary(), term()} | {:ok, binary(), term(), term()} | {:error, term()}
  @type item_router :: (term() -> routed_item())
  @spec route(Topology.t(), list() | map(), item_router(), RequestContext.t()) ::
          {:ok, [map()]} | {:error, term()}
  def route(topology, items, item_router, %RequestContext{} = context) when is_list(items) do
    with :ok <- RequestContext.ensure_active(context) do
      case collect_item_groups(topology, items, item_router, %{}, 0, 0, context) do
        {:error, reason} -> {:error, reason}
        groups -> finalize_active_groups(groups, context)
      end
    end
  end

  def route(topology, items, item_router, %RequestContext{} = context) when is_map(items) do
    collector = fn item, index, groups ->
      route_item_group(topology, item, index, item_router, groups)
    end

    with :ok <- RequestContext.ensure_active(context),
         {:ok, groups} <- BatchMapCollector.collect(items, collector, context),
         do: finalize_active_groups(groups, context)
  end

  defp collect_item_groups(_topology, [], _router, groups, _index, _until_check, _context),
    do: groups

  defp collect_item_groups(topology, items, router, groups, index, 0, ctx) do
    with :ok <- RequestContext.ensure_active(ctx) do
      collect_item_groups(topology, items, router, groups, index, @deadline_check_interval, ctx)
    end
  end

  defp collect_item_groups(
         topology,
         [item | items],
         item_router,
         groups,
         index,
         until_check,
         context
       ) do
    case route_item_group(topology, item, index, item_router, groups) do
      {:cont, groups} ->
        collect_item_groups(
          topology,
          items,
          item_router,
          groups,
          index + 1,
          until_check - 1,
          context
        )

      {:halt, {:error, _reason} = error} ->
        error
    end
  end

  defp collect_item_groups(_topology, _tail, _router, _groups, _index, _check, context) do
    with :ok <- RequestContext.ensure_active(context),
         do: {:error, {:invalid_batch_items, :improper_list}}
  end

  defp route_item_group(topology, item, index, item_router, groups) do
    case BatchItemRouter.call(item_router, item) do
      {:ok, key, prepared_item} when is_binary(key) ->
        put_item_group(topology, key, prepared_item, index, groups)

      {:ok, key, prepared_item, grouping} when is_binary(key) ->
        BatchGroupedRouter.put(topology, key, prepared_item, index, groups, grouping)

      {:error, _reason} = error ->
        {:halt, error}

      other ->
        {:halt, {:error, {:invalid_routed_item, other}}}
    end
  end

  defp put_item_group(topology, key, item, index, groups) do
    case Topology.route_key(topology, key) do
      {:ok, route} ->
        group_key = {route.endpoint_key, route.lane_id}
        group = Map.get(groups, group_key, %{route: route, items: [], indexes: []})

        {:cont,
         Map.put(groups, group_key, %{
           group
           | items: [item | group.items],
             indexes: [index | group.indexes]
         })}

      {:error, reason} ->
        {:halt, {:error, reason}}
    end
  end

  defp finalize_groups(groups) do
    groups =
      groups
      |> Map.values()
      |> Enum.map(fn group ->
        %{group | items: Enum.reverse(group.items), indexes: Enum.reverse(group.indexes)}
      end)
      |> Enum.sort_by(fn %{indexes: [index | _]} -> index end)

    {:ok, groups}
  end

  defp finalize_active_groups(groups, context) do
    with :ok <- RequestContext.ensure_active(context), do: finalize_groups(groups)
  end
end
