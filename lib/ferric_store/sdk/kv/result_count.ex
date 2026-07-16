defmodule FerricStore.SDK.KV.ResultCount do
  @moduledoc false

  alias FerricStore.RequestLimits

  @max_items RequestLimits.max_batch_items()

  @spec validate(atom(), term()) :: :ok | {:error, {:invalid_kv_result_count, map()}}
  def validate(_operation, value)
      when is_integer(value) and value >= 0 and value <= @max_items,
      do: :ok

  def validate(operation, value) do
    {:error, {:invalid_kv_result_count, %{operation: operation, value: value, limit: @max_items}}}
  end
end
