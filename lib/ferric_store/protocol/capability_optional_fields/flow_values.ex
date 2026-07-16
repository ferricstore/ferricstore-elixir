defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowValues do
  @moduledoc false

  @fields %{
    "FLOW.VALUE.PUT" => [
      "partition_key",
      "owner_flow_id",
      "name",
      "ttl_ms",
      "override",
      "local_cache"
    ],
    "FLOW.VALUE.MGET" => ["max_bytes", "payload_max_bytes", "value_max_bytes"]
  }

  @spec all() :: map()
  def all, do: @fields
end
