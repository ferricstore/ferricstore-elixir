defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowOrchestration do
  @moduledoc false

  @fields %{
    "FLOW.STEP_CONTINUE" => [
      "partition_key",
      "lease_ms",
      "worker",
      "payload",
      "payload_ref",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "now_ms"
    ],
    "FLOW.START_AND_CLAIM" => [
      "lease_ms",
      "payload",
      "payload_ref",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "partition_key",
      "parent_flow_id",
      "root_flow_id",
      "correlation_id",
      "priority",
      "retention_ttl_ms",
      "max_active_ms",
      "history_hot_max_events",
      "history_max_events",
      "now_ms"
    ],
    "FLOW.RUN_STEPS_MANY" => [
      "states",
      "steps",
      "lease_ms",
      "payload",
      "result",
      "retention_ttl_ms",
      "now_ms"
    ],
    "FLOW.SIGNAL" => [
      "partition_key",
      "idempotency_key",
      "if_state",
      "transition_to",
      "run_at_ms",
      "now_ms",
      "values",
      "value_refs",
      "drop_values",
      "override_values"
    ],
    "FLOW.SPAWN_CHILDREN" => [
      "wait",
      "wait_state",
      "success",
      "failure",
      "from_state",
      "lease_token",
      "on_child_failed",
      "on_parent_closed",
      "max_active_ms",
      "now_ms"
    ],
    "FLOW.RETENTION_CLEANUP" => ["limit", "now_ms"]
  }

  @spec all() :: map()
  def all, do: @fields
end
