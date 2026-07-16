defmodule FerricStore.Flow.Options.MutationSchema do
  @moduledoc false

  @transport [:timeout, :call_timeout, :lane_id]
  @codec [:codec]

  @terminal [
    :lease_token,
    :fencing_token,
    :now_ms,
    :partition_key,
    :payload,
    :ttl_ms,
    :attributes,
    :attributes_merge,
    :attributes_delete,
    :state_meta,
    :values,
    :value_refs,
    :drop_values,
    :override_values
  ]

  @schemas %{
    create:
      {[:type],
       [
         :type,
         :state,
         :now_ms,
         :run_at_ms,
         :partition_key,
         :payload,
         :payload_ref,
         :parent_flow_id,
         :root_flow_id,
         :correlation_id,
         :priority,
         :idempotent,
         :retention_ttl_ms,
         :max_active_ms,
         :history_hot_max_events,
         :history_max_events,
         :attributes,
         :state_meta,
         :values,
         :value_refs
       ] ++ @codec ++ @transport},
    create_many:
      {[:type],
       [
         :type,
         :state,
         :now_ms,
         :run_at_ms,
         :partition_key,
         :independent,
         :return_ok_on_success,
         :idempotent,
         :priority,
         :retention_ttl_ms,
         :attributes,
         :state_meta,
         :values,
         :value_refs
       ] ++ @codec ++ @transport},
    transition:
      {[:fencing_token, :from_state, :lease_token, :to_state],
       [
         :from_state,
         :to_state,
         :lease_token,
         :fencing_token,
         :now_ms,
         :partition_key,
         :payload,
         :run_at_ms,
         :priority,
         :attributes,
         :attributes_merge,
         :attributes_delete,
         :state_meta,
         :values,
         :value_refs,
         :drop_values,
         :override_values
       ] ++ @codec ++ @transport},
    complete: {[:fencing_token, :lease_token], [:result | @terminal] ++ @codec ++ @transport},
    complete_many:
      {[],
       [
         :now_ms,
         :partition_key,
         :independent,
         :return_ok_on_success,
         :result,
         :payload,
         :ttl_ms,
         :attributes_merge,
         :attributes_delete,
         :state_meta,
         :values,
         :value_refs,
         :drop_values,
         :override_values
       ] ++ @codec ++ @transport},
    retry:
      {[:fencing_token, :lease_token],
       [
         :lease_token,
         :fencing_token,
         :now_ms,
         :partition_key,
         :error,
         :payload,
         :run_at_ms,
         :retry,
         :attributes,
         :attributes_merge,
         :attributes_delete,
         :state_meta
       ] ++ @codec ++ @transport},
    fail: {[:fencing_token, :lease_token], [:error | @terminal] ++ @codec ++ @transport},
    cancel:
      {[:fencing_token],
       [
         :fencing_token,
         :now_ms,
         :lease_token,
         :partition_key,
         :reason,
         :ttl_ms,
         :attributes,
         :attributes_merge,
         :attributes_delete,
         :state_meta,
         :values,
         :value_refs,
         :drop_values,
         :override_values
       ] ++ @codec ++ @transport},
    signal:
      {[:signal],
       [
         :signal,
         :now_ms,
         :partition_key,
         :idempotency_key,
         :if_state,
         :transition_to,
         :run_at_ms,
         :values,
         :value_refs,
         :drop_values,
         :override_values
       ] ++ @codec ++ @transport}
  }

  @spec fetch!(atom()) :: {[atom()], [atom()]}
  def fetch!(operation), do: Map.fetch!(@schemas, operation)
end
