defmodule FerricStore.Flow.QueryIndex do
  @moduledoc """
  Stable identity and query-relevant storage metadata for one index generation.

  Lifecycle progress and statistics remain available in `raw`.
  """

  alias FerricStore.Flow.QueryIndexFormat

  @enforce_keys [
    :id,
    :version,
    :build_id,
    :state,
    :queryable,
    :covering_fields,
    :format,
    :raw
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          id: binary(),
          version: pos_integer(),
          build_id: binary(),
          state: binary(),
          queryable: boolean(),
          covering_fields: [binary()],
          format: QueryIndexFormat.t(),
          raw: map()
        }
end
