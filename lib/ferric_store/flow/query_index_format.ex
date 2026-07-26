defmodule FerricStore.Flow.QueryIndexFormat do
  @moduledoc """
  Opaque derived-storage codec identities for one query-index generation.

  `counter` is `nil` when the generation has no exact count prefix.
  """

  @enforce_keys [:query_row, :key, :entry, :reverse, :counter, :raw]
  defstruct [:query_row, :key, :entry, :reverse, :counter, :raw]

  @type t :: %__MODULE__{
          query_row: binary(),
          key: binary(),
          entry: binary(),
          reverse: binary(),
          counter: binary() | nil,
          raw: map()
        }
end
