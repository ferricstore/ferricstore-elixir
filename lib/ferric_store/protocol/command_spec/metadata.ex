defmodule FerricStore.Protocol.CommandSpec.Metadata do
  @moduledoc false

  @read_operations MapSet.new([
                     :ping,
                     :client_info,
                     :route,
                     :shards,
                     :backpressure,
                     :options,
                     :route_batch,
                     :get,
                     :mget,
                     :hget,
                     :hmget,
                     :hgetall,
                     :lrange,
                     :smembers,
                     :sismember,
                     :zrange,
                     :zscore,
                     :flow_get,
                     :flow_history,
                     :flow_value_mget,
                     :flow_info,
                     :flow_policy_get,
                     :flow_schedule_get,
                     :flow_schedule_list,
                     :flow_stats,
                     :flow_attributes,
                     :flow_attribute_values,
                     :flow_query,
                     :flow_effect_get,
                     :flow_governance_ledger,
                     :flow_approval_get,
                     :flow_circuit_get,
                     :flow_budget_get,
                     :flow_limit_get,
                     :flow_approval_list,
                     :flow_governance_overview,
                     :flow_budget_list,
                     :flow_limit_list,
                     :cluster_health,
                     :cluster_stats,
                     :cluster_keyslot,
                     :cluster_slots,
                     :cluster_status,
                     :cluster_role,
                     :ferricstore_key_info,
                     :ferricstore_hotness,
                     :ferricstore_metrics
                   ])

  @control_operations MapSet.new([
                        :hello,
                        :auth,
                        :ping,
                        :client_set_name,
                        :client_info,
                        :route,
                        :shards,
                        :backpressure,
                        :quit,
                        :goaway,
                        :options,
                        :startup,
                        :window_update,
                        :route_batch,
                        :event,
                        :subscribe_events,
                        :unsubscribe_events
                      ])

  @batch_collections %{
    pipeline: %{field: "commands", atom_field: :commands, type: :list},
    del: %{field: "keys", atom_field: :keys, type: :list},
    mget: %{field: "keys", atom_field: :keys, type: :list},
    mset: %{field: "pairs", atom_field: :pairs, type: :list},
    hset: %{field: "fields", atom_field: :fields, type: :map},
    hmget: %{field: "fields", atom_field: :fields, type: :list},
    lpush: %{field: "values", atom_field: :values, type: :list},
    rpush: %{field: "values", atom_field: :values, type: :list},
    sadd: %{field: "members", atom_field: :members, type: :list},
    srem: %{field: "members", atom_field: :members, type: :list},
    zadd: %{field: "items", atom_field: :items, type: :list},
    zrem: %{field: "members", atom_field: :members, type: :list},
    flow_value_mget: %{field: "refs", atom_field: :refs, type: :list},
    flow_create_many: %{field: "items", atom_field: :items, type: :list},
    flow_complete_many: %{field: "items", atom_field: :items, type: :list},
    flow_transition_many: %{field: "items", atom_field: :items, type: :list},
    flow_retry_many: %{field: "items", atom_field: :items, type: :list},
    flow_fail_many: %{field: "items", atom_field: :items, type: :list},
    flow_cancel_many: %{field: "items", atom_field: :items, type: :list},
    flow_run_steps_many: %{field: "items", atom_field: :items, type: :list}
  }

  @spec control_lane?(atom()) :: boolean()
  def control_lane?(id), do: MapSet.member?(@control_operations, id)

  @spec read_only?(atom()) :: boolean()
  def read_only?(id), do: MapSet.member?(@read_operations, id)

  @spec batch(atom()) :: map() | nil
  def batch(id), do: Map.get(@batch_collections, id)
end
