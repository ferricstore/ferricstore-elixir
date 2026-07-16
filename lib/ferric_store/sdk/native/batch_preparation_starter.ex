defmodule FerricStore.SDK.Native.BatchPreparationStarter do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchOperation,
    BatchPreparer,
    BatchScheduler,
    CoordinatorTimers,
    TopologyManager
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec start(State.t(), BatchOperation.t()) ::
          {:ok, State.t()}
          | {:error, :timeout | {:batch_preparer_start_failed, term()}, State.t()}
  def start(state, batch) do
    if CoordinatorTimers.expired?(batch.opts),
      do: {:error, :timeout, state},
      else: start_active(state, batch)
  end

  defp start_active(state, batch) do
    token = make_ref()

    operation = %BatchPreparer{
      owner: self(),
      token: token,
      batch_id: batch.id,
      topology: TopologyManager.topology(state.topology_manager),
      items: batch.items,
      key_fun: batch.key_fun,
      payload_builder: batch.payload_builder,
      group_preparer: batch.group_preparer,
      item_restorer: batch.item_restorer,
      mode: batch.preparation_mode,
      context: batch.opts
    }

    case start_worker(state.operation_supervisor, operation) do
      {:ok, preparer} ->
        preparer_monitor = Process.monitor(preparer)
        timer = batch.timer || CoordinatorTimers.batch_timer(batch.id, batch.opts)
        caller_monitor = batch.caller_monitor || Process.monitor(elem(batch.from, 0))

        batch = %{
          batch
          | phase: :preparing,
            timer: timer,
            caller_monitor: caller_monitor,
            preparer: %{pid: preparer, monitor: preparer_monitor, token: token}
        }

        state =
          state
          |> put_batch(batch)
          |> State.put_lifecycle_monitor(caller_monitor, {:batch, batch.id})
          |> State.put_lifecycle_monitor(preparer_monitor, {:batch_preparer, batch.id})

        {:ok, state}

      {:error, reason} ->
        {:error, {:batch_preparer_start_failed, reason}, state}
    end
  end

  defp put_batch(state, batch),
    do: %{state | batch_scheduler: BatchScheduler.put(state.batch_scheduler, batch)}

  defp start_worker(supervisor, operation) do
    BatchPreparer.start(supervisor, operation)
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
