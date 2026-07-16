defmodule FerricStore.Protocol.FlowBatchCodec do
  @moduledoc false

  alias FerricStore.Protocol.{FlowCompleteBatchCodec, FlowCreateBatchCodec}

  defdelegate create_many_payload(payload), to: FlowCreateBatchCodec
  defdelegate create_many_iodata_payload(payload), to: FlowCreateBatchCodec
  defdelegate create_many_iodata_payload(payload, item_count), to: FlowCreateBatchCodec

  defdelegate create_many_ids_payload(type, state, partition_key, ids, opts \\ []),
    to: FlowCreateBatchCodec

  defdelegate create_many_ids_iodata_payload(type, state, partition_key, ids, opts \\ []),
    to: FlowCreateBatchCodec

  defdelegate complete_many_payload(payload), to: FlowCompleteBatchCodec, as: :payload

  defdelegate complete_many_iodata_payload(payload),
    to: FlowCompleteBatchCodec,
    as: :iodata_payload

  defdelegate complete_many_iodata_payload(payload, item_count),
    to: FlowCompleteBatchCodec,
    as: :iodata_payload
end
