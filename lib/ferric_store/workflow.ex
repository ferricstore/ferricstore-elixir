defmodule FerricStore.Workflow do
  @moduledoc """
  State-machine workflow convenience API.

  This layer keeps workflows explicit: start a flow in a state, claim due work,
  transition to the next state, or finish with complete/fail/retry.
  """

  alias FerricStore.Flow

  defstruct [
    :client,
    :type,
    initial_state: "queued",
    worker: "elixir-workflow",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  def new(client, type, opts \\ []) do
    struct(__MODULE__, Keyword.merge([client: client, type: type], opts))
  end

  def start(%__MODULE__{} = workflow, id, opts \\ []) do
    Flow.create(
      workflow.client,
      id,
      Keyword.merge(
        [type: workflow.type, state: workflow.initial_state, codec: workflow.codec],
        opts
      )
    )
  end

  def claim(%__MODULE__{} = workflow, state, opts \\ []) do
    Flow.claim_due(
      workflow.client,
      workflow.type,
      Keyword.merge([state: state, worker: workflow.worker, lease_ms: workflow.lease_ms], opts)
    )
  end

  def transition(%__MODULE__{} = workflow, id, from_state, to_state, opts) do
    Flow.transition(
      workflow.client,
      id,
      Keyword.merge([from_state: from_state, to_state: to_state, codec: workflow.codec], opts)
    )
  end

  def complete(%__MODULE__{} = workflow, id, opts) do
    Flow.complete(workflow.client, id, Keyword.merge([codec: workflow.codec], opts))
  end

  def retry(%__MODULE__{} = workflow, id, opts) do
    Flow.retry(workflow.client, id, Keyword.merge([codec: workflow.codec], opts))
  end

  def fail(%__MODULE__{} = workflow, id, opts) do
    Flow.fail(workflow.client, id, Keyword.merge([codec: workflow.codec], opts))
  end
end
