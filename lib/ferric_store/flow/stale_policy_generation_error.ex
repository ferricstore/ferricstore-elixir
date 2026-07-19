defmodule FerricStore.Flow.StalePolicyGenerationError do
  @moduledoc """
  Returned when a Flow policy compare-and-swap uses a stale generation.

  The SDK never retries a policy mutation carrying `expected_generation`.
  Read the latest snapshot and deliberately reconcile the update instead.
  """

  defexception message: "ERR stale flow policy generation",
               expected_generation: nil,
               raw: nil

  @type t :: %__MODULE__{
          message: binary(),
          expected_generation: non_neg_integer() | nil,
          raw: term()
        }
end
