defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowSchedules do
  @moduledoc false

  @fields %{
    "FLOW.SCHEDULE.CREATE" => [
      "kind",
      "at_ms",
      "delay_ms",
      "start_at_ms",
      "every_ms",
      "cron",
      "timezone",
      "now_ms",
      "overwrite",
      "overlap_policy",
      "overlap_retry_ms",
      "max_fires",
      "end_at_ms"
    ],
    "FLOW.SCHEDULE.GET" => [],
    "FLOW.SCHEDULE.FIRE" => ["now_ms", "fire_at_ms"],
    "FLOW.SCHEDULE.PAUSE" => ["now_ms"],
    "FLOW.SCHEDULE.RESUME" => ["now_ms"],
    "FLOW.SCHEDULE.DELETE" => ["now_ms"],
    "FLOW.SCHEDULE.FIRE_DUE" => ["now_ms", "worker", "limit", "lease_ms", "block_ms"],
    "FLOW.SCHEDULE.LIST" => [
      "state",
      "kind",
      "target_type",
      "timezone",
      "from_ms",
      "to_ms",
      "count",
      "rev"
    ]
  }

  @spec all() :: map()
  def all, do: @fields
end
