defmodule FerricStore.SDK.Native.BatchRetry do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchOperation, KVBatchRestorer}

  @spec prepare(BatchOperation.t(), term()) :: {:ok, BatchOperation.t()}
  def prepare(%BatchOperation{} = batch, original_reason) do
    {items, item_restorer, preparation_mode} = retry_inputs(batch)

    {:ok,
     %{
       batch
       | items: items,
         item_restorer: item_restorer,
         preparation_mode: preparation_mode,
         attempt: 1,
         original_reason: original_reason,
         phase: :refreshing,
         connections_remaining: 0,
         connections_inflight: 0,
         connecting_groups: %{},
         connection_queue: [],
         ready_groups: [],
         queued: [],
         inflight: 0,
         request_tags: MapSet.new(),
         successes: [],
         failures: []
     }}
  end

  @spec release_inputs(BatchOperation.t()) :: BatchOperation.t()
  def release_inputs(%BatchOperation{item_restorer: %KVBatchRestorer{}} = batch),
    do: %{batch | items: nil, item_restorer: nil, preparation_mode: :compact}

  def release_inputs(%BatchOperation{attempt: attempt} = batch) when attempt > 0,
    do: %{batch | items: nil, item_restorer: nil}

  def release_inputs(%BatchOperation{} = batch), do: batch

  defp retry_inputs(%BatchOperation{items: groups, item_restorer: %KVBatchRestorer{} = restorer})
       when is_list(groups),
       do: {groups, restorer, :restore_compact}

  defp retry_inputs(%BatchOperation{items: items, preparation_mode: mode}) when is_list(items),
    do: {items, nil, mode}

  defp retry_inputs(%BatchOperation{item_count: item_count, operation: operation} = batch) do
    groups =
      batch.failures ++
        batch.ready_groups ++
        batch.queued ++ batch.connection_queue ++ Map.values(batch.connecting_groups)

    {groups, KVBatchRestorer.new(item_count, operation), :restore_compact}
  end
end
