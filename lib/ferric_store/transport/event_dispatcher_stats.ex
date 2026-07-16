defmodule FerricStore.Transport.EventDispatcherStats do
  @moduledoc false

  alias FerricStore.Transport.{EventDispatcherQueue, EventDispatcherShutdown}

  @spec build(map()) :: map()
  def build(state) do
    %{
      alive: true,
      worker: state.worker,
      busy: not is_nil(state.busy),
      queue_length: EventDispatcherQueue.size(state),
      max_queue: state.max_queue,
      dropped: state.dropped,
      processed: state.processed,
      failed: state.failed,
      stopping: EventDispatcherShutdown.stopping?(state)
    }
  end

  @spec record_callback_outcome(map(), :ok | {:error, term()}) :: map()
  def record_callback_outcome(state, :ok),
    do: Map.update!(state, :processed, &(&1 + 1))

  def record_callback_outcome(state, {:error, _reason}),
    do: Map.update!(state, :failed, &(&1 + 1))

  @spec record_worker_failure(map()) :: map()
  def record_worker_failure(%{busy: nil} = state), do: state
  def record_worker_failure(state), do: Map.update!(state, :failed, &(&1 + 1))
end
