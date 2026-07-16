defmodule FerricStore.SDK.Native.KVBatchRestorer do
  @moduledoc false

  alias FerricStore.Protocol.PreparedMap
  alias FerricStore.{RequestContext, RequestLimits}

  @deadline_check_interval 256
  @max_items RequestLimits.max_batch_items()

  @enforce_keys [:item_count, :operation]
  defstruct @enforce_keys

  @type operation :: :del | :mget | :mset | nil
  @type t :: %__MODULE__{item_count: non_neg_integer(), operation: operation()}

  @spec new(non_neg_integer(), operation()) :: t()
  def new(item_count, operation)
      when is_integer(item_count) and item_count >= 0 and item_count <= @max_items and
             (is_nil(operation) or operation in [:del, :mget, :mset]),
      do: %__MODULE__{item_count: item_count, operation: operation}

  @spec restore(t(), [map()], RequestContext.t()) ::
          {:ok, list()} | {:error, :invalid_prepared_groups | :timeout}
  def restore(
        %__MODULE__{item_count: item_count, operation: operation},
        groups,
        %RequestContext{} = context
      )
      when is_list(groups) do
    with :ok <- RequestContext.ensure_active(context) do
      missing = make_ref()
      items = :array.new(item_count, default: missing, fixed: true)

      groups
      |> restore_groups(items, missing, item_count, operation, 0, 0, context)
      |> finish(item_count, context)
    end
  end

  def restore(%__MODULE__{}, _groups, %RequestContext{}),
    do: {:error, :invalid_prepared_groups}

  defp finish({:ok, items, item_count}, item_count, context) do
    with :ok <- RequestContext.ensure_active(context),
         restored = :array.to_list(items),
         :ok <- RequestContext.ensure_active(context) do
      {:ok, restored}
    end
  end

  defp finish({:error, :timeout} = error, _item_count, _context), do: error
  defp finish(_invalid, _item_count, _context), do: {:error, :invalid_prepared_groups}

  defp restore_groups([], items, _missing, _count, _operation, restored, _check, context) do
    with :ok <- RequestContext.ensure_active(context), do: {:ok, items, restored}
  end

  defp restore_groups(groups, items, missing, count, operation, restored, 0, context) do
    with :ok <- RequestContext.ensure_active(context) do
      restore_groups(
        groups,
        items,
        missing,
        count,
        operation,
        restored,
        @deadline_check_interval,
        context
      )
    end
  end

  defp restore_groups(
         [group | groups],
         items,
         missing,
         count,
         operation,
         restored,
         check,
         context
       ) do
    with {:ok, items, restored} <-
           restore_group(group, items, missing, count, restored, operation, context) do
      restore_groups(
        groups,
        items,
        missing,
        count,
        operation,
        restored,
        check - 1,
        context
      )
    end
  end

  defp restore_groups(
         _improper_tail,
         _items,
         _missing,
         _count,
         _operation,
         _restored,
         _check,
         _context
       ),
       do: :error

  defp restore_group(
         %{indexes: indexes} = group,
         items,
         missing,
         count,
         restored,
         operation,
         context
       )
       when is_list(indexes) do
    with {:ok, group_items} <- group_items(group, operation) do
      restore_group_items(indexes, group_items, items, missing, count, restored, 0, context)
    end
  end

  defp restore_group(_group, _items, _missing, _count, _restored, _operation, _context),
    do: :error

  defp group_items(%{items: items}, nil) when is_list(items), do: {:ok, items}

  defp group_items(%{payload: %PreparedMap{} = payload}, operation)
       when operation in [:del, :mget, :mset] do
    case PreparedMap.metadata(payload) do
      %{operation: ^operation, items: items} when is_list(items) -> {:ok, items}
      _invalid -> :error
    end
  end

  defp group_items(_group, _operation), do: :error

  defp restore_group_items([], [], items, _missing, _count, restored, _check, context) do
    with :ok <- RequestContext.ensure_active(context), do: {:ok, items, restored}
  end

  defp restore_group_items(indexes, group_items, items, missing, count, restored, 0, context) do
    with :ok <- RequestContext.ensure_active(context) do
      restore_group_items(
        indexes,
        group_items,
        items,
        missing,
        count,
        restored,
        @deadline_check_interval,
        context
      )
    end
  end

  defp restore_group_items(
         [index | indexes],
         [item | group_items],
         items,
         missing,
         item_count,
         restored,
         check,
         context
       )
       when is_integer(index) and index >= 0 and index < item_count do
    case :array.get(index, items) do
      ^missing ->
        restore_group_items(
          indexes,
          group_items,
          :array.set(index, item, items),
          missing,
          item_count,
          restored + 1,
          check - 1,
          context
        )

      _duplicate ->
        :error
    end
  end

  defp restore_group_items(
         _indexes,
         _group_items,
         _items,
         _missing,
         _item_count,
         _restored,
         _check,
         _context
       ),
       do: :error
end
