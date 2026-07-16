defmodule FerricStore.SDK.Native.BatchRestoredPreparation do
  @moduledoc false

  alias FerricStore.SDK.Native.KVBatchRestorer

  @spec run(map(), function()) :: {:ok, [map()]} | {:error, term()}
  def run(
        %{
          item_restorer: %KVBatchRestorer{} = restorer,
          group_preparer: group_preparer
        } = operation,
        prepare_compact
      )
      when is_function(group_preparer, 1) and is_function(prepare_compact, 6) do
    with {:ok, items} <- KVBatchRestorer.restore(restorer, operation.items, operation.context) do
      prepare_compact.(
        operation.topology,
        items,
        operation.key_fun,
        operation.payload_builder,
        group_preparer,
        operation.context
      )
    end
  end

  def run(_operation, _prepare_compact), do: {:error, :invalid_batch_preparation}
end
