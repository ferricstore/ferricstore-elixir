defmodule FerricStore.Protocol.CommandSpec.FlowProperties do
  @moduledoc false

  @property_operations %{
    type_scoped: [
      :flow_approval_list,
      :flow_attribute_values,
      :flow_attributes,
      :flow_budget_list,
      :flow_governance_overview,
      :flow_info,
      :flow_limit_list,
      :flow_policy_get,
      :flow_policy_set,
      :flow_retention_cleanup,
      :flow_schedule_fire_due,
      :flow_schedule_list,
      :flow_stats
    ],
    schedule: [
      :flow_schedule_create,
      :flow_schedule_delete,
      :flow_schedule_fire,
      :flow_schedule_fire_due,
      :flow_schedule_get,
      :flow_schedule_list,
      :flow_schedule_pause,
      :flow_schedule_resume
    ],
    approval: [
      :flow_approval_approve,
      :flow_approval_get,
      :flow_approval_reject,
      :flow_approval_request
    ],
    governance: [
      :flow_budget_commit,
      :flow_budget_get,
      :flow_budget_release,
      :flow_budget_reserve,
      :flow_circuit_close,
      :flow_circuit_get,
      :flow_circuit_open,
      :flow_limit_get,
      :flow_limit_lease,
      :flow_limit_release,
      :flow_limit_spend
    ],
    many: [
      :flow_create_many,
      :flow_complete_many,
      :flow_transition_many,
      :flow_retry_many,
      :flow_fail_many,
      :flow_cancel_many
    ],
    claim: [:flow_claim_due, :flow_reclaim],
    state_id: [
      :flow_cancel,
      :flow_complete,
      :flow_create,
      :flow_effect_compensate,
      :flow_effect_confirm,
      :flow_effect_fail,
      :flow_effect_get,
      :flow_effect_reserve,
      :flow_extend_lease,
      :flow_fail,
      :flow_get,
      :flow_governance_ledger,
      :flow_history,
      :flow_retry,
      :flow_rewind,
      :flow_signal,
      :flow_spawn_children,
      :flow_start_and_claim,
      :flow_step_continue,
      :flow_transition
    ],
    payload_value: [
      :flow_create,
      :flow_complete,
      :flow_transition,
      :flow_retry,
      :flow_fail,
      :flow_step_continue,
      :flow_start_and_claim,
      :flow_complete_many,
      :flow_transition_many,
      :flow_retry_many,
      :flow_fail_many
    ]
  }

  @option_starts %{
    flow_complete: 2,
    flow_retry: 2,
    flow_fail: 2,
    flow_extend_lease: 2,
    flow_transition: 3,
    flow_step_continue: 4,
    flow_run_steps_many: 0,
    flow_retention_cleanup: 0,
    flow_schedule_fire_due: 0,
    flow_schedule_list: 0,
    flow_approval_list: 0,
    flow_governance_overview: 0,
    flow_budget_list: 0,
    flow_limit_list: 0
  }

  @properties Enum.reduce(@property_operations, %{}, fn {property, operations}, acc ->
                Enum.reduce(operations, acc, fn operation, properties ->
                  Map.update(
                    properties,
                    operation,
                    MapSet.new([property]),
                    &MapSet.put(&1, property)
                  )
                end)
              end)

  @spec for_command(atom()) :: MapSet.t(atom())
  def for_command(command), do: Map.get(@properties, command, MapSet.new())

  @spec option_start(atom()) :: non_neg_integer()
  def option_start(command), do: Map.get(@option_starts, command, 1)
end
