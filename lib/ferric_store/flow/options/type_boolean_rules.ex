defmodule FerricStore.Flow.Options.TypeBooleanRules do
  @moduledoc false

  @ordered_queries [:list, :search, :terminals, :failures, :by_parent, :by_root, :by_correlation]
  @cold_queries [:list, :terminals, :failures, :by_parent, :by_root, :by_correlation]

  def options do
    %{
      consistent_projection: [:history | @ordered_queries],
      full: [:get],
      idempotent: [:create, :create_many],
      include_attributes: [:claim_due],
      include_cold: [:history | @cold_queries],
      include_record: [:claim_due],
      include_state: [:claim_due],
      independent: [:complete_many, :create_many],
      local_cache: [:value_put],
      override: [:value_put],
      payload: [:claim_due, :get],
      reclaim_expired: [:claim_due],
      return_ok_on_success: [:complete_many, :create_many],
      rev: [:history | @ordered_queries],
      terminal_only: [:search],
      values: [:history]
    }
  end
end
