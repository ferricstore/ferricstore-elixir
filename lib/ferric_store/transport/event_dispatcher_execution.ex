defmodule FerricStore.Transport.EventDispatcherExecution do
  @moduledoc false

  alias FerricStore.Transport.{
    EventDispatcherCallerRegistry,
    EventDispatcherProtocol,
    EventDispatcherQueue,
    EventDispatcherShutdown,
    EventDispatcherStats,
    EventDispatcherWorker
  }

  @spec initialize(pid(), (term() -> term()), pos_integer(), timeout()) :: map()
  def initialize(owner, handler, max_queue, commit_timeout) do
    Process.flag(:trap_exit, true)

    %{
      owner: owner,
      owner_monitor: Process.monitor(owner),
      handler: handler,
      worker: nil,
      busy: nil,
      dropped: 0,
      processed: 0,
      failed: 0,
      stopping: MapSet.new(),
      callers: %EventDispatcherCallerRegistry{},
      commit_timeout: commit_timeout
    }
    |> EventDispatcherQueue.initialize(max_queue)
    |> start_worker()
  end

  @spec start_next_event(map()) :: map()
  def start_next_event(%{busy: nil} = state) do
    case EventDispatcherQueue.take_committed(state) do
      {:ok, event, state} -> run_event(state, event)
      {_blocked_or_empty, state} -> state
    end
  end

  def start_next_event(state), do: state

  @spec worker_exit(map()) :: map()
  def worker_exit(state) do
    state
    |> Map.put(:worker, nil)
    |> EventDispatcherStats.record_worker_failure()
    |> Map.put(:busy, nil)
    |> maybe_restart_worker()
    |> start_next_event()
  end

  @spec stop_worker(map()) :: :ok
  def stop_worker(state), do: EventDispatcherWorker.stop(state.worker)

  defp run_event(state, event) do
    state = start_worker(state)
    token = make_ref()
    send(state.worker, {EventDispatcherProtocol, :invoke, self(), token, event})
    %{state | busy: %{token: token}}
  end

  defp start_worker(%{worker: nil, handler: handler} = state) do
    %{state | worker: EventDispatcherWorker.start(self(), handler)}
  end

  defp start_worker(state), do: state

  defp maybe_restart_worker(state) do
    if EventDispatcherShutdown.stopping?(state) and EventDispatcherQueue.empty?(state),
      do: state,
      else: start_worker(state)
  end
end
