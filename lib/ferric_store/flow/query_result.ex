defmodule FerricStore.Flow.QueryResult do
  @moduledoc """
  Typed result returned by `FerricStore.Flow.query/4`.

  Exactly one of `records` or `count` is populated. Pagination cursors are
  opaque and must only be reused with the same query and parameters.
  """

  @enforce_keys [:version, :quality, :usage, :raw]
  defstruct [:version, :records, :page, :count, :quality, :usage, :raw]

  @type t :: %__MODULE__{
          version: binary(),
          records: [map()] | nil,
          page: %{has_more: boolean(), cursor: binary() | nil} | nil,
          count: non_neg_integer() | nil,
          quality: map(),
          usage: map(),
          raw: map()
        }
end
