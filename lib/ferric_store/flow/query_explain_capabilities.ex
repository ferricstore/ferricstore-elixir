defmodule FerricStore.Flow.QueryExplainCapabilities do
  @moduledoc """
  Specialized executor contracts reported by a bounded FQL `EXPLAIN` plan.
  """

  @enforce_keys [:requested, :available, :missing, :raw]
  defstruct [:requested, :available, :missing, :raw]

  @type t :: %__MODULE__{
          requested: [binary()],
          available: [binary()],
          missing: [binary()],
          raw: map()
        }
end
