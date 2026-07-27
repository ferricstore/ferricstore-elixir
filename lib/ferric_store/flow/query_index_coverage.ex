defmodule FerricStore.Flow.QueryIndexCoverage do
  @moduledoc "Shard coverage and validation state for an index generation."

  @enforce_keys [:complete_shards, :total_shards, :validation, :raw]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          complete_shards: non_neg_integer(),
          total_shards: pos_integer(),
          validation: binary(),
          raw: map()
        }
end
