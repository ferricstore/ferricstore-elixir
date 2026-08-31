defmodule FerricStore.Flow.DurableMutationOutcomeUnknownError do
  @moduledoc """
  Reports that a durable `FLOW.STEP_CONTINUE` mutation may already have committed.

  FerricStore never retries the mutation automatically after this error. Recover
  by reading or reclaiming the workflow and use the stable step name to replay a
  committed result; reusing the stale claim for another mutation is unsafe.
  """

  defexception [:operation, :cause, message: "durable workflow mutation outcome is unknown"]

  @type t :: %__MODULE__{
          operation: :flow_step_continue,
          cause: Exception.t(),
          message: binary()
        }

  @impl true
  def exception(opts) do
    operation = Keyword.fetch!(opts, :operation)
    cause = Keyword.fetch!(opts, :cause)

    %__MODULE__{
      operation: operation,
      cause: cause,
      message:
        "#{operation} outcome is unknown; recover with a fresh claim instead of retrying the stale mutation"
    }
  end
end
