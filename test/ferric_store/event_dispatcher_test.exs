defmodule FerricStore.EventDispatcherTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.EventDispatcher
  alias FerricStore.Transport.EventDispatcherProtocol

  test "dispatch admission bounds a blocked handler queue" do
    test_pid = self()

    handler = fn
      :block ->
        send(test_pid, {:handler_blocked, self()})

        receive do
          :release -> :ok
        end

      event ->
        send(test_pid, {:handled, event})
    end

    dispatcher = EventDispatcher.start(self(), handler, max_queue: 32)
    assert :ok = EventDispatcher.dispatch(dispatcher, :block)
    assert_receive {:handler_blocked, worker}, 200
    before_memory = process_memory(dispatcher)

    Enum.each(1..10_000, fn event ->
      assert EventDispatcher.dispatch(dispatcher, event) in [:ok, :dropped_oldest]
    end)

    stats = EventDispatcher.stats(dispatcher)
    assert stats.queue_length == 32
    assert stats.dropped == 9_968
    assert process_memory(dispatcher) - before_memory < 250_000

    send(worker, :release)
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "one isolated worker is reused across successful callbacks" do
    test_pid = self()
    dispatcher = EventDispatcher.start(self(), &send(test_pid, {:handled_by, self(), &1}))

    assert :ok = EventDispatcher.dispatch(dispatcher, :first)
    assert_receive {:handled_by, worker, :first}, 200
    assert :ok = EventDispatcher.dispatch(dispatcher, :second)
    assert_receive {:handled_by, ^worker, :second}, 200

    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "a killed callback worker is replaced without killing the dispatcher" do
    test_pid = self()

    handler = fn
      :kill -> Process.exit(self(), :kill)
      event -> send(test_pid, {:handled_by, self(), event})
    end

    dispatcher = EventDispatcher.start(self(), handler)
    first_worker = EventDispatcher.stats(dispatcher).worker
    assert :ok = EventDispatcher.dispatch(dispatcher, :kill)

    assert_eventually(fn -> EventDispatcher.stats(dispatcher).worker != first_worker end)
    assert :ok = EventDispatcher.dispatch(dispatcher, :after_crash)
    assert_receive {:handled_by, replacement, :after_crash}, 200
    refute replacement == first_worker
    assert Process.alive?(dispatcher)

    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "caught callback exceptions are counted as failures, not processed events" do
    test_pid = self()

    handler = fn
      :raise -> raise "callback failed"
      event -> send(test_pid, {:handled, event})
    end

    dispatcher = EventDispatcher.start(self(), handler)
    assert :ok = EventDispatcher.dispatch(dispatcher, :raise)

    assert_eventually(fn ->
      %{failed: failed, processed: processed} = EventDispatcher.stats(dispatcher)
      failed == 1 and processed == 0
    end)

    assert :ok = EventDispatcher.dispatch(dispatcher, :after_failure)
    assert_receive {:handled, :after_failure}, 200

    assert_eventually(fn ->
      %{failed: failed, processed: processed} = EventDispatcher.stats(dispatcher)
      failed == 1 and processed == 1
    end)

    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "a dispatch reported as dropped cannot execute after admission times out" do
    test_pid = self()
    dispatcher = EventDispatcher.start(self(), &send(test_pid, {:handled, &1}))
    true = :erlang.suspend_process(dispatcher)

    dispatch = Task.async(fn -> EventDispatcher.dispatch(dispatcher, :late, 20) end)

    try do
      assert Task.await(dispatch, 200) == :dropped
    after
      true = :erlang.resume_process(dispatcher)
    end

    refute_receive {:handled, :late}, 100
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "a caller dying before commit cannot block later callbacks" do
    test_pid = self()
    dispatcher = EventDispatcher.start(self(), &send(test_pid, {:handled, &1}))

    {caller, caller_monitor} =
      spawn_monitor(fn ->
        request_ref = make_ref()
        reply_alias = Process.alias()

        send(
          dispatcher,
          {EventDispatcherProtocol, :prepare_dispatch, self(), reply_alias, request_ref,
           :abandoned}
        )

        receive do
          {EventDispatcherProtocol, :reply, ^request_ref, :ok} ->
            send(test_pid, :abandoned_dispatch_prepared)
        end
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)

    assert_receive :abandoned_dispatch_prepared, 200
    assert_receive {:DOWN, ^caller_monitor, :process, ^caller, :normal}, 200
    assert_eventually(fn -> EventDispatcher.stats(dispatcher).queue_length == 0 end)

    assert :ok = EventDispatcher.dispatch(dispatcher, :next)
    assert_receive {:handled, :next}, 200
    refute_receive {:handled, :abandoned}
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "an alive caller that never commits loses its admission lease" do
    test_pid = self()

    dispatcher =
      EventDispatcher.start(self(), &send(test_pid, {:handled, &1}), commit_timeout: 30)

    caller =
      spawn(fn ->
        request_ref = make_ref()
        reply_alias = Process.alias()

        send(
          dispatcher,
          {EventDispatcherProtocol, :prepare_dispatch, self(), reply_alias, request_ref, :stalled}
        )

        receive do
          {EventDispatcherProtocol, :reply, ^request_ref, :ok} ->
            send(test_pid, :stalled_dispatch_prepared)
            Process.sleep(:infinity)
        end
      end)

    on_exit(fn -> if Process.alive?(caller), do: Process.exit(caller, :kill) end)

    assert_receive :stalled_dispatch_prepared, 200
    assert :ok = EventDispatcher.dispatch(dispatcher, :next)
    assert_receive {:handled, :next}, 300
    refute_receive {:handled, :stalled}
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "evicting uncommitted admissions releases their caller monitors immediately" do
    test_pid = self()

    handler = fn
      :block ->
        send(test_pid, {:handler_blocked, self()})

        receive do
          :release -> :ok
        end

      _event ->
        :ok
    end

    dispatcher = EventDispatcher.start(self(), handler, max_queue: 1, commit_timeout: 5_000)
    assert :ok = EventDispatcher.dispatch(dispatcher, :block)
    assert_receive {:handler_blocked, worker}, 200

    Enum.each(1..100, fn event ->
      request_ref = make_ref()
      reply_alias = Process.alias()

      send(
        dispatcher,
        {EventDispatcherProtocol, :prepare_dispatch, self(), reply_alias, request_ref, event}
      )

      assert_receive {EventDispatcherProtocol, :reply, ^request_ref, result}, 200
      assert result in [:ok, :dropped_oldest]
      Process.unalias(reply_alias)
    end)

    assert {:monitors, monitors} = Process.info(dispatcher, :monitors)
    assert Enum.count(monitors, &match?({:process, _pid}, &1)) == 2

    send(worker, :release)
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "timed-out stats requests cannot leave late replies in the caller mailbox" do
    dispatcher = EventDispatcher.start(self(), fn _event -> :ok end)
    true = :erlang.suspend_process(dispatcher)

    assert %{alive: false} = EventDispatcher.stats(dispatcher, 0)
    true = :erlang.resume_process(dispatcher)

    assert %{alive: true} = EventDispatcher.stats(dispatcher, 200)

    refute_receive {FerricStore.Transport.EventDispatcherProtocol, :reply, _request_ref, _result}
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "malformed mailbox protocol messages cannot crash the dispatcher" do
    test_pid = self()
    dispatcher = EventDispatcher.start(self(), &send(test_pid, {:handled, &1}))

    for message <- [
          {EventDispatcherProtocol, :prepare_dispatch, :not_a_pid, make_ref(), make_ref(),
           :event},
          {EventDispatcherProtocol, :prepare_dispatch, self(), :not_an_alias, :not_a_ref, :event},
          {EventDispatcherProtocol, :commit_dispatch, :not_a_ref},
          {EventDispatcherProtocol, :cancel_dispatch, :not_a_ref},
          {FerricStore.Transport.EventDispatcherCallerRegistry, :commit_timeout, :bad, :bad},
          {EventDispatcherProtocol, :request, :not_a_destination, :not_a_ref, :stats},
          {EventDispatcherProtocol, :worker_done, :not_a_pid, :not_a_ref, :ok},
          {EventDispatcherProtocol, :stop, :not_a_pid, :not_a_ref},
          {EventDispatcherProtocol, :force_stop, :not_a_pid, :not_a_ref}
        ] do
      send(dispatcher, message)
    end

    assert %{alive: true} = EventDispatcher.stats(dispatcher, 200)
    assert :ok = EventDispatcher.dispatch(dispatcher, :after_forgery)
    assert_receive {:handled, :after_forgery}, 200
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "dispatcher APIs reject timers outside the portable timeout domain" do
    dispatcher = EventDispatcher.start(self(), fn _event -> :ok end)
    unsafe_timeout = FerricStore.Timeout.max_finite() + 1

    assert :dropped = EventDispatcher.dispatch(dispatcher, :event, unsafe_timeout)
    assert %{alive: false} = EventDispatcher.stats(dispatcher, unsafe_timeout)
    assert :ok = EventDispatcher.stop(dispatcher, unsafe_timeout)
    assert Process.alive?(dispatcher)
    assert :ok = EventDispatcher.stop(dispatcher)
  end

  test "an infinite stop drains and terminates the dispatcher" do
    dispatcher = EventDispatcher.start(self(), fn _event -> :ok end)
    monitor = Process.monitor(dispatcher)

    assert :ok = EventDispatcher.stop(dispatcher, :infinity)
    assert_receive {:DOWN, ^monitor, :process, ^dispatcher, :normal}, 200
  end

  test "a forced stop does not leak its private monitor into the caller mailbox" do
    dispatcher = EventDispatcher.start(self(), fn _event -> :ok end)
    true = :erlang.suspend_process(dispatcher)

    assert :ok = EventDispatcher.stop(dispatcher, 0)
    refute_receive {:DOWN, _monitor, :process, ^dispatcher, _reason}, 200
  end

  test "a forced stop sends only one acknowledgement to its caller" do
    owner = self()

    dispatcher =
      EventDispatcher.start(self(), fn :block ->
        send(owner, {:blocked_handler, self()})
        Process.sleep(:infinity)
      end)

    assert :ok = EventDispatcher.dispatch(dispatcher, :block)
    assert_receive {:blocked_handler, _worker}, 200
    assert :ok = EventDispatcher.stop(dispatcher, 0)

    refute_receive {EventDispatcherProtocol, :stopped, _request_ref}, 200
  end

  test "dispatcher startup bounds and validates its option list before traversal" do
    oversized_opts = List.duplicate({:max_queue, 1}, 100_000)
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    dispatcher = EventDispatcher.start(self(), fn _event -> :ok end, oversized_opts)
    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert after_reductions - before_reductions < 20_000
    assert %{alive: true, max_queue: 1_024} = EventDispatcher.stats(dispatcher)
    assert :ok = EventDispatcher.stop(dispatcher)

    malformed = EventDispatcher.start(self(), fn _event -> :ok end, [:not_an_option])
    assert %{alive: true, max_queue: 1_024} = EventDispatcher.stats(malformed)
    assert :ok = EventDispatcher.stop(malformed)
  end

  defp process_memory(process) do
    {:memory, bytes} = Process.info(process, :memory)
    bytes
  end

  defp assert_eventually(fun, attempts \\ 40)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())
end
