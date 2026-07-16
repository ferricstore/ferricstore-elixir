defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowLifecycle do
  @moduledoc false

  @fields %{
    "FLOW.CREATE" => [
      "type",
      "state",
      "payload",
      "payload_ref",
      "payload_refs",
      "value_refs",
      "attributes",
      "partition_key",
      "parent_flow_id",
      "root_flow_id",
      "correlation_id",
      "run_at_ms",
      "due_after_ms",
      "priority",
      "retention_ttl_ms",
      "max_active_ms"
    ],
    "FLOW.CREATE_MANY" => [
      "partition_key",
      "type",
      "state",
      "payload",
      "payload_ref",
      "attributes",
      "run_at_ms",
      "priority",
      "retention_ttl_ms",
      "max_active_ms",
      "history_hot_max_events",
      "history_max_events",
      "independent",
      "return",
      "now_ms"
    ],
    "FLOW.COMPLETE" => [
      "partition_key",
      "result",
      "payload",
      "ttl_ms",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "state_meta",
      "now_ms"
    ],
    "FLOW.TRANSITION" => [
      "partition_key",
      "payload",
      "payload_ref",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "state_meta",
      "run_at_ms",
      "priority",
      "now_ms"
    ],
    "FLOW.RETRY" => [
      "partition_key",
      "error",
      "payload",
      "run_at_ms",
      "retry",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "state_meta",
      "now_ms"
    ],
    "FLOW.FAIL" => [
      "partition_key",
      "error",
      "payload",
      "ttl_ms",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "state_meta",
      "now_ms"
    ],
    "FLOW.CANCEL" => [
      "lease_token",
      "partition_key",
      "reason",
      "ttl_ms",
      "values",
      "value_refs",
      "drop_values",
      "override_values",
      "attributes",
      "attributes_merge",
      "attributes_delete",
      "state_meta",
      "now_ms"
    ]
  }

  @spec all() :: map()
  def all, do: @fields
end
