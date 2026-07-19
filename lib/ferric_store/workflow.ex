defmodule FerricStore.Workflow do
  @moduledoc """
  State-machine workflow convenience API.

  This layer keeps workflows explicit: start a flow in a state, claim due work,
  transition to the next state, or finish with complete/fail/retry.
  """

  alias FerricStore.Flow
  alias FerricStore.Flow.ConsumerConfig
  alias FerricStore.Flow.Options
  alias FerricStore.Result

  defstruct [
    :client,
    :type,
    initial_state: "queued",
    worker: "elixir-workflow",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  @type t :: %__MODULE__{
          client: pid(),
          type: binary(),
          initial_state: binary(),
          worker: binary(),
          lease_ms: pos_integer(),
          codec: module()
        }

  @config_defaults [
    initial_state: "queued",
    worker: "elixir-workflow",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  def new(client, type, opts \\ []) do
    config = ConsumerConfig.validate!(client, type, opts, @config_defaults)
    struct!(__MODULE__, [client: client, type: type] ++ config)
  end

  def start(%__MODULE__{} = workflow, id, opts \\ []) do
    with_flow_options(
      :create,
      [type: workflow.type, state: workflow.initial_state, codec: workflow.codec],
      opts,
      &Flow.create(workflow.client, id, &1)
    )
  end

  def claim(%__MODULE__{} = workflow, state, opts \\ []) do
    with_flow_options(
      :claim_due,
      [
        state: state,
        worker: workflow.worker,
        lease_ms: workflow.lease_ms,
        include_record: true,
        payload: true,
        codec: workflow.codec
      ],
      opts,
      &Flow.claim_due(workflow.client, workflow.type, &1)
    )
  end

  @doc """
  Installs this workflow's Flow policy.

  Workflow declarations replace the stored policy by default. Pass
  `replace: false` to request an explicit deep patch, and use
  `expected_generation` for compare-and-swap installation.
  """
  @spec install_policy(t(), keyword()) :: FerricStore.Flow.PolicySnapshot.t() | {:error, term()}
  def install_policy(%__MODULE__{} = workflow, opts \\ []) do
    with_flow_options(
      :policy_set,
      [replace: true],
      opts,
      &Flow.policy_set(workflow.client, workflow.type, &1)
    )
  end

  def transition(%__MODULE__{} = workflow, id, from_state, to_state, opts) do
    with_flow_options(
      :transition,
      [from_state: from_state, to_state: to_state, codec: workflow.codec],
      opts,
      &Flow.transition(workflow.client, id, &1)
    )
  end

  def complete(%__MODULE__{} = workflow, id, opts) do
    with_flow_options(
      :complete,
      [codec: workflow.codec],
      opts,
      &Flow.complete(workflow.client, id, &1)
    )
  end

  def retry(%__MODULE__{} = workflow, id, opts) do
    with_flow_options(
      :retry,
      [codec: workflow.codec],
      opts,
      &Flow.retry(workflow.client, id, &1)
    )
  end

  def fail(%__MODULE__{} = workflow, id, opts) do
    with_flow_options(
      :fail,
      [codec: workflow.codec],
      opts,
      &Flow.fail(workflow.client, id, &1)
    )
  end

  defp with_flow_options(operation, defaults, opts, function) do
    case Options.merge(operation, defaults, opts) do
      {:ok, merged} -> function.(merged)
      {:error, reason} -> Result.error(reason)
    end
  end
end
