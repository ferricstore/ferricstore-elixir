defmodule FerricStore.SDK.Native.BatchPreflightCompletion do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchExecution, BatchScheduler}

  @spec finish(map(), reference()) ::
          {:run, map(), reference()} | {:finish, map(), reference()} | {:continue, map()}
  def finish(state, batch_id) do
    case BatchScheduler.get(state.batch_scheduler, batch_id) do
      %{phase: :connecting, connections_remaining: 0, failures: []} = batch ->
        groups = Enum.sort_by(batch.ready_groups, fn %{indexes: [index | _]} -> index end)
        {state, groups} = BatchExecution.release_preflight(state, groups)
        batch = %{batch | phase: :running, ready_groups: [], queued: groups}
        {:run, put_batch(state, batch), batch_id}

      %{phase: :connecting, connections_remaining: 0} ->
        {:finish, state, batch_id}

      _batch_or_nil ->
        {:continue, state}
    end
  end

  defp put_batch(state, batch),
    do: %{state | batch_scheduler: BatchScheduler.put(state.batch_scheduler, batch)}
end
