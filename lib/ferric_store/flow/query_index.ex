defmodule FerricStore.Flow.QueryIndex do
  @moduledoc """
  Stable identity and query-relevant storage metadata for one index generation.

  Lifecycle progress and statistics are decoded into bounded typed sections.
  """

  alias FerricStore.Flow.{
    QueryIndexBuild,
    QueryIndexCoverage,
    QueryIndexField,
    QueryIndexFormat,
    QueryIndexRetirement,
    QueryIndexStatistics,
    QueryIndexValidation
  }

  @enforce_keys [
    :id,
    :version,
    :build_id,
    :source,
    :state,
    :queryable,
    :fields,
    :workloads,
    :count_prefixes,
    :covering_fields,
    :format,
    :coverage,
    :build,
    :validation,
    :retirement,
    :statistics,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          id: binary(),
          version: pos_integer(),
          build_id: binary(),
          source: binary(),
          state: binary(),
          queryable: boolean(),
          fields: [QueryIndexField.t()],
          workloads: [binary()],
          count_prefixes: [pos_integer()],
          covering_fields: [binary()],
          format: QueryIndexFormat.t(),
          coverage: QueryIndexCoverage.t(),
          build: QueryIndexBuild.t(),
          validation: QueryIndexValidation.t(),
          retirement: QueryIndexRetirement.t(),
          statistics: QueryIndexStatistics.t(),
          raw: map()
        }
end
