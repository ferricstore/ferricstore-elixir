defmodule FerricStore.Flow.QueryIndexServices do
  @moduledoc "Availability of the replicated query-index management services."

  @enforce_keys [:registry, :lifecycle_worker, :statistics_store, :statistics_worker, :raw]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          registry: binary(),
          lifecycle_worker: binary(),
          statistics_store: binary(),
          statistics_worker: binary(),
          raw: map()
        }
end
