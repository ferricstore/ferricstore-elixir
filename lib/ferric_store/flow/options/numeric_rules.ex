defmodule FerricStore.Flow.Options.NumericRules do
  @moduledoc false

  @ordered_queries [:list, :search, :terminals, :failures, :by_parent, :by_root, :by_correlation]

  def nonnegative_exact do
    %{
      from_ms: [:history | @ordered_queries],
      from_version: [:history],
      max_bytes: [:value_mget],
      now_ms: [
        :cancel,
        :claim_due,
        :complete,
        :complete_many,
        :create,
        :create_many,
        :fail,
        :retry,
        :signal,
        :transition,
        :value_put,
        :stuck
      ],
      older_than_ms: [:stuck],
      payload_max_bytes: [:claim_due, :get, :history, :value_mget],
      run_at_ms: [:create, :create_many, :retry, :signal, :transition],
      to_ms: [:history | @ordered_queries],
      to_version: [:history],
      value_max_bytes: [:claim_due, :value_mget]
    }
  end

  def positive_exact do
    %{
      count: @ordered_queries ++ [:history, :stuck],
      lease_ms: [:claim_due],
      limit: [:claim_due],
      ttl_ms: [:cancel, :complete, :complete_many, :fail, :value_put]
    }
  end

  def positive_signed, do: %{retention_ttl_ms: [:create, :create_many]}

  def special do
    %{
      block_ms: {[:claim_due], :unsigned_32},
      history_hot_max_events: {[:create], :history_hot},
      history_max_events: {[:create], :history},
      max_active_ms: {[:create, :create_many], :max_active},
      priority: {[:claim_due, :create, :create_many, :transition], :priority},
      reclaim_ratio: {[:claim_due], :percentage}
    }
  end
end
