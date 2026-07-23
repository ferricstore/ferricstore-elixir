defmodule FerricStore.Flow do
  @moduledoc """
  High-level FerricFlow command helpers.

  Functions build the same native command arguments as the Python SDK while
  keeping defaults simple: create and terminal commands return acknowledgements,
  while claim returns compact jobs with attributes.
  """

  alias FerricStore.Flow.{
    BatchCommands,
    LifecycleCommands,
    Payload,
    PolicyCommand,
    PolicyCommands,
    QueryCommands,
    ValueCommands
  }

  def create(client, id, opts), do: LifecycleCommands.create(client, id, opts)
  def enqueue(client, id, opts), do: LifecycleCommands.enqueue(client, id, opts)
  def transition(client, id, opts), do: LifecycleCommands.transition(client, id, opts)
  def complete(client, id, opts), do: LifecycleCommands.complete(client, id, opts)
  def retry(client, id, opts), do: LifecycleCommands.retry(client, id, opts)
  def fail(client, id, opts), do: LifecycleCommands.fail(client, id, opts)
  def cancel(client, id, opts), do: LifecycleCommands.cancel(client, id, opts)
  def signal(client, id, opts), do: LifecycleCommands.signal(client, id, opts)

  def create_many(client, items, opts), do: BatchCommands.create_many(client, items, opts)
  def complete_many(client, jobs, opts \\ []), do: BatchCommands.complete_many(client, jobs, opts)

  def get(client, id, opts \\ []), do: QueryCommands.get(client, id, opts)
  def list(client, opts \\ []), do: QueryCommands.list(client, opts)
  def history(client, id, opts \\ []), do: QueryCommands.history(client, id, opts)
  def claim_due(client, type, opts), do: QueryCommands.claim_due(client, type, opts)
  def search(client, opts \\ []), do: QueryCommands.search(client, opts)
  def terminals(client, type, opts \\ []), do: QueryCommands.terminals(client, type, opts)
  def failures(client, type, opts \\ []), do: QueryCommands.failures(client, type, opts)
  def by_parent(client, id, opts \\ []), do: QueryCommands.by_parent(client, id, opts)
  def by_root(client, id, opts \\ []), do: QueryCommands.by_root(client, id, opts)

  def by_correlation(client, id, opts \\ []),
    do: QueryCommands.by_correlation(client, id, opts)

  def stuck(client, type, opts \\ []), do: QueryCommands.stuck(client, type, opts)

  def query(client, query, params \\ %{}, opts \\ []),
    do: QueryCommands.query(client, query, params, opts)

  def explain(client, query, params \\ %{}, opts \\ []),
    do: QueryCommands.explain(client, query, params, opts)

  def explain_analyze(client, query, params \\ %{}, opts \\ []),
    do: QueryCommands.explain_analyze(client, query, params, opts)

  def query_indexes(client, index_id \\ nil, opts \\ []),
    do: QueryCommands.query_indexes(client, index_id, opts)

  @doc """
  Deep-patches a Flow type policy and returns its typed snapshot.

  Pass `replace: true` for full replacement. Pass `expected_generation` from a
  prior `FerricStore.Flow.PolicySnapshot` for compare-and-swap. CAS mutations
  are never retried automatically.
  """
  @spec policy_set(pid(), binary(), keyword()) ::
          FerricStore.Flow.PolicySnapshot.t()
          | {:error, FerricStore.Error.t() | FerricStore.Flow.StalePolicyGenerationError.t()}
  def policy_set(client, type, opts \\ []), do: PolicyCommands.set(client, type, opts)

  @doc "Returns the typed policy snapshot and its monotonic `generation`."
  @spec policy_get(pid(), binary(), keyword()) ::
          FerricStore.Flow.PolicySnapshot.t() | {:error, FerricStore.Error.t()}
  def policy_get(client, type, opts \\ []), do: PolicyCommands.get(client, type, opts)
  def value_put(client, value, opts \\ []), do: ValueCommands.put(client, value, opts)
  def value_mget(client, refs, opts \\ []), do: ValueCommands.mget(client, refs, opts)

  def get_payload(id, opts \\ []), do: Payload.get_payload(id, opts)
  def history_payload(id, opts \\ []), do: Payload.history_payload(id, opts)
  def create_payload(id, opts), do: Payload.create_payload(id, opts)
  def create_many_payload(items, opts), do: Payload.create_many_payload(items, opts)
  def claim_due_payload(type, opts), do: Payload.claim_due_payload(type, opts)
  def transition_payload(id, opts), do: Payload.transition_payload(id, opts)
  def complete_payload(id, opts), do: Payload.complete_payload(id, opts)
  def retry_payload(id, opts), do: Payload.retry_payload(id, opts)
  def fail_payload(id, opts), do: Payload.fail_payload(id, opts)
  def cancel_payload(id, opts), do: Payload.cancel_payload(id, opts)
  def signal_payload(id, opts), do: Payload.signal_payload(id, opts)
  def complete_many_payload(jobs, opts \\ []), do: Payload.complete_many_payload(jobs, opts)

  def policy_set_payload(type, opts), do: unwrap_policy(PolicyCommand.set_payload(type, opts))

  def policy_get_payload(type, opts \\ []),
    do: unwrap_policy(PolicyCommand.get_payload(type, opts))

  defp unwrap_policy({:ok, payload}), do: payload
  defp unwrap_policy({:error, _reason} = error), do: error
end
