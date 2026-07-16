defmodule FerricStore.SDK.KV.MultiKeyPolicy do
  @moduledoc false

  alias FerricStore.RequestContext

  @spec put(:del | :mset, non_neg_integer(), RequestContext.t()) :: RequestContext.t()
  def put(_operation, item_count, context) when item_count <= 1, do: context

  def put(:mset, _item_count, context) do
    case RequestContext.option(context, :atomicity) do
      :per_slot -> context
      _default -> RequestContext.put_option(context, :require_same_slot, :mset)
    end
  end

  def put(:del, _item_count, context) do
    if RequestContext.option(context, :atomicity) == :per_shard,
      do: context,
      else: RequestContext.put_option(context, :require_same_shard, :del)
  end
end
