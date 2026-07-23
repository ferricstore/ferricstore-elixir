defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowQueries do
  @moduledoc false

  @fields %{
    "FLOW.GET" => ["partition_key", "full", "payload", "payload_max_bytes", "values"],
    "FLOW.QUERY" => ["params"],
    "FLOW.CLAIM_DUE" => [
      "states",
      "limit",
      "lease_ms",
      "worker",
      "partition_key",
      "partition_keys",
      "block_ms",
      "reclaim_expired",
      "reclaim_ratio",
      "return"
    ],
    "FLOW.HISTORY" => [
      "partition_key",
      "count",
      "from_event",
      "to_event",
      "from_ms",
      "to_ms",
      "from_version",
      "to_version",
      "rev",
      "event",
      "worker",
      "values",
      "payload_max_bytes",
      "include_cold",
      "consistent_projection"
    ],
    "FLOW.STATS" => [
      "state",
      "attributes",
      "partition_key",
      "count",
      "consistent_projection"
    ],
    "FLOW.ATTRIBUTES" => ["state", "partition_key", "count", "consistent_projection"],
    "FLOW.ATTRIBUTE_VALUES" => [
      "state",
      "partition_key",
      "count",
      "consistent_projection"
    ]
  }

  @spec all() :: map()
  def all, do: @fields
end
