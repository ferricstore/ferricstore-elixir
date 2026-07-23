defmodule FerricStore.Flow.QueryExplainResult do
  @moduledoc """
  Value-redacted physical plan returned by FQL `EXPLAIN` or `EXPLAIN ANALYZE`.
  """

  @enforce_keys [:version, :query_fingerprint, :status, :plan, :estimate, :bounds, :raw]
  defstruct [
    :version,
    :query_fingerprint,
    :status,
    :plan,
    :estimate,
    :bounds,
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
          bounds: map(),
          actual: map() | nil,
          diagnostic: FerricStore.Flow.QueryError.t() | nil,
          raw: map()
        }
end
