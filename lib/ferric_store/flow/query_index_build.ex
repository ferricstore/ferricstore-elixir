defmodule FerricStore.Flow.QueryIndexBuild do
  @moduledoc "Durable build progress for an index generation."

  @enforce_keys [
    :scope,
    :phase_counts,
    :current_phases,
    :completed_shards,
    :total_shards,
    :scanned_records,
    :written_entries,
    :written_bytes,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          scope: binary(),
          phase_counts: %{binary() => non_neg_integer()},
          current_phases: [binary()],
          completed_shards: non_neg_integer(),
          total_shards: pos_integer(),
          scanned_records: non_neg_integer(),
          written_entries: non_neg_integer(),
          written_bytes: non_neg_integer(),
          raw: map()
        }
end
