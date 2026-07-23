defmodule FerricStore.Flow.QueryIndexStatus do
  @moduledoc """
  Bounded OSS query-index catalog returned by `FLOW.QUERY.INDEXES`.
  """

  @enforce_keys [
    :contract_version,
    :observed_at_ms,
    :statistics_max_age_ms,
    :registry,
    :services,
    :indexes,
    :raw
  ]
  defstruct [
    :contract_version,
    :observed_at_ms,
    :statistics_max_age_ms,
    :registry,
    :services,
    :indexes,
    :raw
  ]

  @type t :: %__MODULE__{
          contract_version: binary(),
          observed_at_ms: non_neg_integer(),
          statistics_max_age_ms: non_neg_integer(),
          registry: map(),
          services: map(),
          indexes: [map()],
          raw: map()
        }
end
