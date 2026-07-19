defmodule FerricStore.Flow.Options.QuerySchema do
  @moduledoc false

  @transport [:timeout, :call_timeout, :lane_id]
  @codec [:codec]

  @schemas %{
    get:
      {[], [:partition_key, :full, :payload, :payload_max_bytes, :values] ++ @codec ++ @transport},
    list:
      {[:type],
       [
         :type,
         :state,
         :partition_key,
         :count,
         :from_ms,
         :to_ms,
         :rev,
         :attributes,
         :include_cold,
         :consistent_projection,
         :return
       ] ++ @codec ++ @transport},
    history:
      {[],
       [
         :partition_key,
         :count,
         :from_event,
         :to_event,
         :from_ms,
         :to_ms,
         :from_version,
         :to_version,
         :rev,
         :event,
         :worker,
         :values,
         :payload_max_bytes,
         :include_cold,
         :consistent_projection
       ] ++ @codec ++ @transport},
    claim_due:
      {[:worker],
       [
         :worker,
         :lease_ms,
         :limit,
         :now_ms,
         :state,
         :states,
         :partition_key,
         :partition_keys,
         :priority,
         :block_ms,
         :payload,
         :payload_max_bytes,
         :values,
         :value_max_bytes,
         :reclaim_expired,
         :reclaim_ratio,
         :include_record,
         :include_state,
         :include_attributes
       ] ++ @codec ++ @transport},
    policy_set:
      {[],
       [
         :replace,
         :expected_generation,
         :indexed_state_meta,
         :indexed_attributes,
         :max_active_ms,
         :retry,
         :retention,
         :states
       ] ++
         @transport},
    policy_get: {[], [:state | @transport]},
    search:
      {[:type],
       [
         :type,
         :state,
         :partition_key,
         :count,
         :from_ms,
         :to_ms,
         :rev,
         :terminal_only,
         :consistent_projection,
         :attributes,
         :state_meta
       ] ++ @codec ++ @transport},
    value_put:
      {[],
       [
         :now_ms,
         :partition_key,
         :owner_flow_id,
         :name,
         :override,
         :ttl_ms,
         :local_cache
       ] ++ @codec ++ @transport},
    value_mget: {[], [:max_bytes, :value_max_bytes, :payload_max_bytes] ++ @codec ++ @transport}
  }

  @spec fetch!(atom()) :: {[atom()], [atom()]}
  def fetch!(operation), do: Map.fetch!(@schemas, operation)
end
