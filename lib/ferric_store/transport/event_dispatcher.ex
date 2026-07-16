defmodule FerricStore.Transport.EventDispatcher do
  @moduledoc false

  alias FerricStore.Transport.{
    EventDispatcherAdmission,
    EventDispatcherCallerRegistry,
    EventDispatcherCallerRuntime,
    EventDispatcherClient,
    EventDispatcherExecution,
    EventDispatcherOptions,
    EventDispatcherProtocol,
    EventDispatcherQueue,
    EventDispatcherShutdown,
    EventDispatcherStats
  }

  @default_drain_timeout 1_000
  @default_max_queue 1_024
  @request_timeout 1_000
  @type dispatch_result :: EventDispatcherProtocol.dispatch_result()

  @spec start(pid(), (term() -> term()), keyword()) :: pid()
  def start(owner, handler, opts \\ [])
      when is_pid(owner) and is_function(handler, 1) and is_list(opts) do
    {max_queue, commit_timeout} =
      EventDispatcherOptions.parse(opts, @default_max_queue, @request_timeout)

    spawn(fn -> init(owner, handler, max_queue, commit_timeout) end)
  end

  @spec dispatch(pid(), term()) :: dispatch_result()
  def dispatch(dispatcher, event) when is_pid(dispatcher),
    do: dispatch(dispatcher, event, @request_timeout)

  @spec dispatch(pid(), term(), timeout()) :: dispatch_result()
  def dispatch(dispatcher, event, timeout) when is_pid(dispatcher),
    do: EventDispatcherClient.dispatch(dispatcher, event, timeout)

  @spec stats(pid(), timeout()) :: map()
  def stats(dispatcher, timeout \\ @request_timeout) when is_pid(dispatcher),
    do: EventDispatcherClient.request(dispatcher, :stats, timeout, %{alive: false})

  @spec barrier(pid(), timeout()) :: :ok | :unavailable
  def barrier(dispatcher, timeout \\ @request_timeout) when is_pid(dispatcher),
    do: EventDispatcherClient.request(dispatcher, :barrier, timeout, :unavailable)

  @spec stop(pid(), timeout()) :: :ok
  def stop(dispatcher, timeout \\ @default_drain_timeout) when is_pid(dispatcher),
    do: EventDispatcherClient.stop(dispatcher, timeout)

  defp init(owner, handler, max_queue, commit_timeout) do
    owner
    |> EventDispatcherExecution.initialize(handler, max_queue, commit_timeout)
    |> loop()
  end

  defp loop(state) do
    receive do
      message -> handle_message(state, message)
    end
  end

  defp handle_message(
         state,
         {EventDispatcherProtocol, :prepare_dispatch, caller, reply_to, request_ref, event}
       )
       when is_pid(caller) and is_reference(reply_to) and is_reference(request_ref) do
    {state, result} = EventDispatcherAdmission.prepare(state, caller, request_ref, event)
    reply(reply_to, request_ref, result)
    continue_or_stop(state)
  end

  defp handle_message(state, {EventDispatcherProtocol, :commit_dispatch, request_ref})
       when is_reference(request_ref) do
    state
    |> EventDispatcherCallerRuntime.release(request_ref)
    |> EventDispatcherQueue.commit(request_ref)
    |> EventDispatcherExecution.start_next_event()
    |> continue_or_stop()
  end

  defp handle_message(state, {EventDispatcherProtocol, :cancel_dispatch, request_ref})
       when is_reference(request_ref) do
    state
    |> EventDispatcherCallerRuntime.release(request_ref)
    |> EventDispatcherQueue.cancel(request_ref)
    |> EventDispatcherExecution.start_next_event()
    |> continue_or_stop()
  end

  defp handle_message(
         state,
         {EventDispatcherCallerRegistry, :commit_timeout, request_ref, monitor}
       )
       when is_reference(request_ref) and is_reference(monitor) do
    state
    |> EventDispatcherCallerRuntime.expire(request_ref, monitor)
    |> EventDispatcherExecution.start_next_event()
    |> continue_or_stop()
  end

  defp handle_message(
         state,
         {EventDispatcherProtocol, :request, caller, request_ref, :stats}
       )
       when (is_pid(caller) or is_reference(caller)) and is_reference(request_ref) do
    reply(caller, request_ref, EventDispatcherStats.build(state))
    loop(state)
  end

  defp handle_message(
         state,
         {EventDispatcherProtocol, :request, caller, request_ref, :barrier}
       )
       when (is_pid(caller) or is_reference(caller)) and is_reference(request_ref) do
    reply(caller, request_ref, :ok)
    loop(state)
  end

  defp handle_message(
         state,
         {EventDispatcherProtocol, :worker_done, worker, token, outcome}
       )
       when is_pid(worker) and is_reference(token) do
    case state do
      %{worker: ^worker, busy: %{token: ^token}} ->
        state
        |> Map.put(:busy, nil)
        |> EventDispatcherStats.record_callback_outcome(outcome)
        |> EventDispatcherExecution.start_next_event()
        |> continue_or_stop()

      _stale_completion ->
        loop(state)
    end
  end

  defp handle_message(state, {:EXIT, worker, _reason}) when worker == state.worker do
    state |> EventDispatcherExecution.worker_exit() |> continue_or_stop()
  end

  defp handle_message(state, {:DOWN, monitor, :process, owner, _reason})
       when monitor == state.owner_monitor and owner == state.owner do
    EventDispatcherExecution.stop_worker(state)
  end

  defp handle_message(state, {:DOWN, monitor, :process, _caller, _reason}) do
    state
    |> EventDispatcherCallerRuntime.down(monitor)
    |> EventDispatcherExecution.start_next_event()
    |> continue_or_stop()
  end

  defp handle_message(state, {EventDispatcherProtocol, :stop, caller, request_ref})
       when is_pid(caller) and is_reference(request_ref) do
    Process.demonitor(state.owner_monitor, [:flush])

    state
    |> EventDispatcherShutdown.add_waiter(caller, request_ref)
    |> EventDispatcherCallerRuntime.clear()
    |> EventDispatcherQueue.drop_uncommitted()
    |> EventDispatcherExecution.start_next_event()
    |> continue_or_stop()
  end

  defp handle_message(state, {EventDispatcherProtocol, :force_stop, caller, request_ref})
       when is_pid(caller) and is_reference(request_ref) do
    Process.demonitor(state.owner_monitor, [:flush])

    state
    |> EventDispatcherShutdown.add_waiter(caller, request_ref)
    |> EventDispatcherShutdown.finish()
  end

  defp handle_message(state, _message), do: loop(state)

  defp continue_or_stop(%{busy: nil} = state) do
    if EventDispatcherShutdown.stopping?(state) and EventDispatcherQueue.empty?(state),
      do: EventDispatcherShutdown.finish(state),
      else: loop(state)
  end

  defp continue_or_stop(state), do: loop(state)

  defp reply(caller, request_ref, result),
    do: send(caller, {EventDispatcherProtocol, :reply, request_ref, result})
end
