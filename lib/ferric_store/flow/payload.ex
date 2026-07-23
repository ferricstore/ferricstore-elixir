defmodule FerricStore.Flow.Payload do
  @moduledoc false

  alias FerricStore.Flow.Payload.{Batch, Mutation, Query}

  def get_payload(id, opts \\ []), do: Query.get_payload(id, opts)
  def history_payload(id, opts \\ []), do: Query.history_payload(id, opts)
  def claim_due_payload(type, opts), do: Query.claim_due_payload(type, opts)

  def create_payload(id, opts), do: Mutation.create_payload(id, opts)
  def transition_payload(id, opts), do: Mutation.transition_payload(id, opts)
  def complete_payload(id, opts), do: Mutation.complete_payload(id, opts)
  def retry_payload(id, opts), do: Mutation.retry_payload(id, opts)
  def fail_payload(id, opts), do: Mutation.fail_payload(id, opts)
  def cancel_payload(id, opts), do: Mutation.cancel_payload(id, opts)
  def signal_payload(id, opts), do: Mutation.signal_payload(id, opts)

  def create_many_payload(items, opts), do: Batch.create_many_payload(items, opts)
  def create_many_with_count(items, opts), do: Batch.create_many_with_count(items, opts)

  def create_many_with_count(items, opts, budget),
    do: Batch.create_many_with_count(items, opts, budget)

  def complete_many_payload(jobs, opts \\ []), do: Batch.complete_many_payload(jobs, opts)
  def complete_many_with_count(jobs, opts), do: Batch.complete_many_with_count(jobs, opts)

  def complete_many_with_count(jobs, opts, budget),
    do: Batch.complete_many_with_count(jobs, opts, budget)
end
