defmodule FerricStore.SDK.Native.EventQueueTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.EventQueue

  test "cancelled calls do not leave an unbounded FIFO tombstone backlog" do
    head = %{id: make_ref(), value: :head}
    tail = %{id: make_ref(), value: :tail}
    queue = EventQueue.enqueue(%EventQueue{}, head)

    queue =
      Enum.reduce(1..20_000, queue, fn _index, queue ->
        cancelled = %{id: make_ref(), value: :cancelled}
        queue = EventQueue.enqueue(queue, cancelled)
        {^cancelled, queue} = EventQueue.pop(queue, cancelled.id)
        queue
      end)

    queue = EventQueue.enqueue(queue, tail)

    assert EventQueue.size(queue) == 2
    assert :queue.len(queue.order) < 100

    {{:value, ^head}, queue} = EventQueue.out(queue)
    {:reductions, before} = Process.info(self(), :reductions)
    {{:value, ^tail}, queue} = EventQueue.out(queue)
    {:reductions, after_out} = Process.info(self(), :reductions)

    assert after_out - before < 10_000
    assert {:empty, %EventQueue{}} = EventQueue.out(queue)
  end

  test "compaction preserves FIFO order among live calls" do
    calls = Enum.map(1..200, &%{id: make_ref(), value: &1})
    queue = Enum.reduce(calls, %EventQueue{}, &EventQueue.enqueue(&2, &1))

    queue =
      calls
      |> Enum.reject(&(rem(&1.value, 10) == 0))
      |> Enum.reduce(queue, fn call, queue ->
        {^call, queue} = EventQueue.pop(queue, call.id)
        queue
      end)

    assert drain(queue, []) == Enum.filter(calls, &(rem(&1.value, 10) == 0))
  end

  test "re-enqueuing one call id preserves the original without growing the FIFO" do
    id = make_ref()

    queue =
      Enum.reduce(1..10_000, %EventQueue{}, fn value, queue ->
        EventQueue.enqueue(queue, %{id: id, value: value})
      end)

    assert EventQueue.size(queue) == 1
    assert :queue.len(queue.order) == 1
    assert {{:value, %{id: ^id, value: 1}}, %EventQueue{}} = EventQueue.out(queue)
  end

  defp drain(queue, acc) do
    case EventQueue.out(queue) do
      {{:value, call}, queue} -> drain(queue, [call | acc])
      {:empty, _queue} -> Enum.reverse(acc)
    end
  end
end
