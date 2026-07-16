defmodule FerricStore.SDK.Native.KVPreparedRequest do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.KVBatchPreparer

  @enforce_keys [
    :reservation,
    :opcode,
    :operation,
    :item_count,
    :topology_version,
    :groups,
    :opts
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          reservation: reference() | nil,
          opcode: non_neg_integer(),
          operation: KVBatchPreparer.operation(),
          item_count: non_neg_integer(),
          topology_version: reference(),
          groups: [map()],
          opts: RequestContext.t()
        }

  @spec new(
          reference() | nil,
          non_neg_integer(),
          KVBatchPreparer.operation(),
          non_neg_integer(),
          reference(),
          [map()],
          RequestContext.t()
        ) :: t()
  def new(
        reservation,
        opcode,
        operation,
        item_count,
        topology_version,
        groups,
        %RequestContext{} = context
      )
      when (is_nil(reservation) or is_reference(reservation)) and
             is_integer(opcode) and opcode >= 0 and operation in [:del, :mget, :mset] and
             is_integer(item_count) and item_count >= 0 and is_reference(topology_version) and
             is_list(groups) do
    %__MODULE__{
      reservation: reservation,
      opcode: opcode,
      operation: operation,
      item_count: item_count,
      topology_version: topology_version,
      groups: groups,
      opts: context
    }
  end
end
