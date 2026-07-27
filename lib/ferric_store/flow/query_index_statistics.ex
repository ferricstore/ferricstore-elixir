defmodule FerricStore.Flow.QueryIndexStatistics do
  @moduledoc "Freshness summary for bounded query-index statistics samples."

  @enforce_keys [
    :status,
    :samples,
    :fresh_samples,
    :stale_samples,
    :future_samples,
    :oldest_collected_at_ms,
    :newest_collected_at_ms,
    :oldest_age_ms,
    :newest_age_ms,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          status: binary(),
          samples: non_neg_integer(),
          fresh_samples: non_neg_integer(),
          stale_samples: non_neg_integer(),
          future_samples: non_neg_integer(),
          oldest_collected_at_ms: non_neg_integer() | nil,
          newest_collected_at_ms: non_neg_integer() | nil,
          oldest_age_ms: non_neg_integer() | nil,
          newest_age_ms: non_neg_integer() | nil,
          raw: map()
        }
end
