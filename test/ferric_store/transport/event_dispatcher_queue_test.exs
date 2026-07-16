defmodule FerricStore.Transport.EventDispatcherQueueTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.EventDispatcherQueue

  test "duplicate admission references cannot bypass the bounded queue" do
    request_ref = make_ref()

    state =
      %{busy: nil, dropped: 0}
      |> EventDispatcherQueue.initialize(8)
      |> repeat_admission(request_ref, 10_000)

    assert EventDispatcherQueue.size(state) == 1
    assert :queue.len(state.queue) == 1
    assert state.dropped == 9_999
  end

  test "a duplicate reference cannot replace an existing admission" do
    request_ref = make_ref()
    state = EventDispatcherQueue.initialize(%{busy: nil, dropped: 0}, 8)

    {state, :ok, nil} = EventDispatcherQueue.prepare(state, request_ref, :original)
    {state, :dropped, nil} = EventDispatcherQueue.prepare(state, request_ref, :replacement)
    state = EventDispatcherQueue.commit(state, request_ref)

    assert {:ok, :original, state} = EventDispatcherQueue.take_committed(state)
    assert EventDispatcherQueue.empty?(state)
    assert state.dropped == 1
  end

  defp repeat_admission(state, request_ref, count) do
    Enum.reduce(1..count, state, fn event, state ->
      {state, _result, _evicted_request_ref} =
        EventDispatcherQueue.prepare(state, request_ref, event)

      state
    end)
  end
end
