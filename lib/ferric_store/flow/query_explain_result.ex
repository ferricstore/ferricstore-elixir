defmodule FerricStore.Flow.QueryExplainResult do
  @moduledoc """
  Value-redacted physical plan returned by FQL `EXPLAIN` or `EXPLAIN ANALYZE`.
  """

  @enforce_keys [
    :version,
    :query_fingerprint,
    :status,
    :plan,
    :estimate,
    :capabilities,
    :stats,
    :quality,
    :bounds,
    :pressure,
    :decision,
    :alternatives,
    :raw
  ]
  defstruct [
    :version,
    :query_fingerprint,
    :status,
    :plan,
    :estimate,
    :capabilities,
    :stats,
    :quality,
    :bounds,
    :pressure,
    :decision,
    :alternatives,
    :actual,
    :diagnostic,
    :raw
  ]

  @type t :: %__MODULE__{
          version: binary(),
          query_fingerprint: binary(),
          status: binary(),
          plan: map(),
          estimate: map(),
          capabilities: FerricStore.Flow.QueryExplainCapabilities.t() | nil,
          stats: map() | nil,
          quality: map() | nil,
          bounds: map(),
          pressure: map() | nil,
          decision: map() | nil,
          alternatives: [map()],
          actual: map() | nil,
          diagnostic: FerricStore.Flow.QueryError.t() | nil,
          raw: map()
        }
end
