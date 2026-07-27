defmodule FerricStore.Flow.QueryIndexValidation do
  @moduledoc "Exact validation progress for an index generation."

  @enforce_keys [
    :scope,
    :status,
    :phase_counts,
    :current_phases,
    :completed_shards,
    :total_shards,
    :checked_records,
    :checked_entries,
    :mismatches,
    :failure_reason,
    :validated_at_ms,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          scope: binary(),
          status: binary(),
          phase_counts: %{binary() => non_neg_integer()},
          current_phases: [binary()],
          completed_shards: non_neg_integer(),
          total_shards: pos_integer(),
          checked_records: non_neg_integer(),
          checked_entries: non_neg_integer(),
          mismatches: non_neg_integer(),
          failure_reason: binary() | nil,
          validated_at_ms: non_neg_integer() | nil,
          raw: map()
        }
end
