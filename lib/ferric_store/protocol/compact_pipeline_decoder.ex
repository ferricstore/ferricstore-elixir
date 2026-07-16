defmodule FerricStore.Protocol.CompactPipelineDecoder do
  @moduledoc false

  alias FerricStore.Protocol.CompactPipelineItems
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()

  @spec decode(binary()) :: {:ok, [list()]} | {:error, term()}
  def decode(<<count::32, rest::binary>>) when count <= @max_collection_items,
    do: CompactPipelineItems.decode(count, rest)

  def decode(<<_count::32, _rest::binary>>), do: {:error, :collection_too_large}
  def decode(_payload), do: {:error, :invalid_compact_pipeline}

  @spec decode(binary(), [atom()]) :: {:ok, [list()]} | {:error, term()}
  def decode(<<count::32, rest::binary>>, plan)
      when count <= @max_collection_items and is_list(plan) do
    CompactPipelineItems.decode(count, rest, plan)
  end

  def decode(<<count::32, _rest::binary>>, _plan) when count > @max_collection_items,
    do: {:error, :collection_too_large}

  def decode(<<_count::32, _rest::binary>>, _plan), do: {:error, :invalid_compact_pipeline_plan}
  def decode(_payload, _plan), do: {:error, :invalid_compact_pipeline}
end
