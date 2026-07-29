defmodule FerricStore.SDK.Native.ResponseDecoderSpawnPolicy do
  @moduledoc false

  @flow_query_opcode FerricStore.Protocol.Opcodes.flow_query()
  @preallocation_threshold_bytes 4 * 1_024
  @query_min_heap_words 2_000

  @spec options(non_neg_integer(), term()) :: keyword()
  def options(@flow_query_opcode, body_bytes)
      when is_integer(body_bytes) and body_bytes >= @preallocation_threshold_bytes,
      do: [min_heap_size: @query_min_heap_words]

  def options(_opcode, _body_bytes), do: []
end
