defmodule FerricStore.SDK.Native.BatchCompletion do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchPolicy, BatchPreflightReservations, BatchScheduler}
  alias FerricStore.SDK.Native.Coordinator.State

  @spec take(State.t(), reference()) ::
          {{:ok, [map()]} | {:retry, term()} | {:error, term()}, State.t(), map()}
  def take(%State{} = state, batch_id) when is_reference(batch_id) do
    {batch, batch_scheduler} = BatchScheduler.pop(state.batch_scheduler, batch_id)

    state =
      state
      |> Map.put(:batch_scheduler, batch_scheduler)
      |> BatchPreflightReservations.release(batch)

    successes = BatchPolicy.sort_results(batch.successes)
    failures = BatchPolicy.sort_results(batch.failures)
    {BatchPolicy.completion(batch, successes, failures), state, batch}
  end
end
