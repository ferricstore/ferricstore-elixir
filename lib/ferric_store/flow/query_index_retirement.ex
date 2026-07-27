defmodule FerricStore.Flow.QueryIndexRetirement do
  @moduledoc "Durable cleanup progress for a retiring index generation."

  @enforce_keys [
    :status,
    :phase_counts,
    :current_phases,
    :completed_shards,
    :total_shards,
    :deleted_entries,
    :deleted_bytes,
    :rewritten_reverse_rows,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          status: binary(),
          phase_counts: %{binary() => non_neg_integer()} | nil,
          current_phases: [binary()] | nil,
          completed_shards: non_neg_integer() | nil,
          total_shards: pos_integer() | nil,
          deleted_entries: non_neg_integer() | nil,
          deleted_bytes: non_neg_integer() | nil,
          rewritten_reverse_rows: non_neg_integer() | nil,
          raw: map()
        }
end
