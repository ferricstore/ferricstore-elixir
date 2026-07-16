defmodule FerricStore.SDK.Native.RefreshCompletionQueueTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{RefreshCompletionQueue, RefreshOperation}

  test "completion work drains in bounded FIFO chunks" do
    {operation, waiters} = operation_with_waiters(130)
    queue = RefreshCompletionQueue.new() |> RefreshCompletionQueue.enqueue(operation, :ok)

    {first, queue} = RefreshCompletionQueue.take(queue, 64)
    {second, queue} = RefreshCompletionQueue.take(queue, 64)
    {third, queue} = RefreshCompletionQueue.take(queue, 64)

    assert Enum.map(first, &elem(&1, 0)) == Enum.take(waiters, 64)
    assert Enum.map(second, &elem(&1, 0)) == waiters |> Enum.drop(64) |> Enum.take(64)
    assert Enum.map(third, &elem(&1, 0)) == Enum.drop(waiters, 128)
    assert Enum.all?(first ++ second ++ third, &(elem(&1, 1) == :ok))
    assert RefreshCompletionQueue.empty?(queue)
  end

  test "a completed waiter can be cancelled before its chunk is resumed" do
    operation = new_operation()
    first = {:request_retry, make_ref()}

    cancelled =
      {:refresh_call, {self(), make_ref()}, make_ref(), nil,
       FerricStore.RequestContext.new([], 100)}

    last = {:batch_retry, make_ref()}

    operation = add_waiters(operation, [first, cancelled, last])
    queue = RefreshCompletionQueue.new() |> RefreshCompletionQueue.enqueue(operation, :ok)

    assert {:ok, queue} =
             RefreshCompletionQueue.cancel(queue, RefreshOperation.waiter_key(cancelled))

    {completions, queue} = RefreshCompletionQueue.take(queue, 64)

    assert Enum.map(completions, &elem(&1, 0)) == [first, last]
    assert RefreshCompletionQueue.empty?(queue)

    assert RefreshCompletionQueue.cancel(queue, RefreshOperation.waiter_key(cancelled)) ==
             :missing
  end

  test "cancelled waiters consume the bounded drain budget" do
    cancelled = Enum.map(1..100_000, fn _index -> {:request_retry, make_ref()} end)
    active = {:batch_retry, make_ref()}

    operation = %{
      new_operation()
      | waiters: [active | Enum.reverse(cancelled)],
        waiter_count: 1,
        waiter_keys: MapSet.new([RefreshOperation.waiter_key(active)]),
        cancelled_waiters: cancelled |> Enum.map(&RefreshOperation.waiter_key/1) |> MapSet.new()
    }

    queue = RefreshCompletionQueue.new() |> RefreshCompletionQueue.enqueue(operation, :ok)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    {completions, queue} = RefreshCompletionQueue.take(queue, 64)
    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert completions == []
    refute RefreshCompletionQueue.empty?(queue)
    assert after_reductions - before_reductions < 20_000
  end

  test "cancelling a completion does not scan unrelated completion groups" do
    waiters = Enum.map(1..10_000, fn _index -> {:request_retry, make_ref()} end)

    queue =
      Enum.reduce(waiters, RefreshCompletionQueue.new(), fn waiter, queue ->
        RefreshCompletionQueue.enqueue_waiters(queue, [waiter], :ok)
      end)

    cancelled = List.last(waiters)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:ok, queue} =
             RefreshCompletionQueue.cancel(queue, RefreshOperation.waiter_key(cancelled))

    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert after_reductions - before_reductions < 5_000

    assert RefreshCompletionQueue.cancel(queue, RefreshOperation.waiter_key(cancelled)) ==
             :missing
  end

  test "duplicate active completion keys are coalesced across queued groups" do
    waiter = {:request_retry, make_ref()}

    queue =
      RefreshCompletionQueue.new()
      |> RefreshCompletionQueue.enqueue_waiters([waiter], :first)
      |> RefreshCompletionQueue.enqueue_waiters([waiter], :duplicate)

    assert RefreshCompletionQueue.active_waiters(queue) == [waiter]
    assert {[{^waiter, :first}], queue} = RefreshCompletionQueue.take(queue, 64)
    assert RefreshCompletionQueue.empty?(queue)
  end

  test "an older cancellation tombstone cannot make a newer active completion uncancellable" do
    waiter = {:request_retry, make_ref()}
    key = RefreshOperation.waiter_key(waiter)

    tombstone = %{
      new_operation()
      | waiters: [waiter],
        waiter_count: 0,
        waiter_keys: MapSet.new(),
        cancelled_waiters: MapSet.new([key])
    }

    queue =
      RefreshCompletionQueue.new()
      |> RefreshCompletionQueue.enqueue(tombstone, :old)
      |> RefreshCompletionQueue.enqueue_waiters([waiter], :new)

    assert {:ok, queue} = RefreshCompletionQueue.cancel(queue, key)
    assert RefreshCompletionQueue.cancel(queue, key) == :missing
    assert {[], queue} = RefreshCompletionQueue.take(queue, 64)
    assert RefreshCompletionQueue.empty?(queue)
  end

  defp operation_with_waiters(count) do
    waiters = Enum.map(1..count, fn _index -> {:request_retry, make_ref()} end)
    operation = add_waiters(new_operation(), waiters)
    {operation, waiters}
  end

  defp new_operation do
    RefreshOperation.new(self(), make_ref(), make_ref(), false)
  end

  defp add_waiters(operation, waiters) do
    Enum.reduce(waiters, operation, fn waiter, operation ->
      {operation, true} = RefreshOperation.add(operation, waiter)
      operation
    end)
  end
end
