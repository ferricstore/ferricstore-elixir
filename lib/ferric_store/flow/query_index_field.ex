defmodule FerricStore.Flow.QueryIndexField do
  @moduledoc "One component of a composite query-index key."

  @enforce_keys [:name, :direction, :encoding, :raw]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          name: binary(),
          direction: binary(),
          encoding: binary(),
          raw: map()
        }
end
