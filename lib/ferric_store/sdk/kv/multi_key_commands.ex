defmodule FerricStore.SDK.KV.MultiKeyCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{BatchResults, Input, MultiKeyPolicy}
  alias FerricStore.SDK.Native.KVRequests

  def del(client, key_or_keys, context) do
    with {:ok, keys, item_count} <-
           Input.route_key_or_list(key_or_keys, :del, RequestContext.budget(context), true) do
      dispatch_del(client, keys, item_count, context)
    end
  end

  def mget(client, keys, context) do
    with {:ok, keys, item_count} <-
           Input.route_key_list(keys, :mget, RequestContext.budget(context), true) do
      dispatch_mget(client, keys, item_count, context)
    end
  end

  def mset(client, pairs, context) do
    with {:ok, pairs, item_count} <- Input.mset_pairs(pairs, RequestContext.budget(context)),
         context = MultiKeyPolicy.put(:mset, item_count, context),
         {:ok, groups, ^item_count} <- dispatch_mset(client, pairs, item_count, context) do
      BatchResults.mset(groups, item_count, RequestContext.budget(context))
    end
  end

  defp dispatch_del(_client, _keys, 0, _context), do: {:ok, 0}

  defp dispatch_del(client, keys, item_count, context) do
    context = MultiKeyPolicy.put(:del, item_count, context)

    case KVRequests.request_items(client, :del, Opcodes.del(), keys, item_count, context) do
      {:ok, groups, ^item_count} ->
        BatchResults.del(groups, item_count, RequestContext.budget(context))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp dispatch_mget(_client, _keys, 0, _context), do: {:ok, []}

  defp dispatch_mget(client, keys, item_count, context) do
    case KVRequests.request_items(client, :mget, Opcodes.mget(), keys, item_count, context) do
      {:ok, groups, ^item_count} ->
        BatchResults.mget(groups, item_count, RequestContext.budget(context))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp dispatch_mset(_client, _pairs, 0, _context), do: {:ok, [], 0}

  defp dispatch_mset(client, pairs, item_count, context),
    do: KVRequests.request_items(client, :mset, Opcodes.mset(), pairs, item_count, context)
end
