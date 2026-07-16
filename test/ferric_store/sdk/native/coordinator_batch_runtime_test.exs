defmodule FerricStore.SDK.Native.CoordinatorBatchRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{BatchScheduler, CoordinatorBatchRuntime}
  alias FerricStore.SDK.Native.Coordinator.State

  test "bounded endpoint wakeups continue while batches make progress" do
    endpoint_key = :ready_endpoint
    state = waiting_batches(100, endpoint_key)

    state =
      CoordinatorBatchRuntime.resume_waiting_endpoint(
        state,
        endpoint_key,
        64,
        fn state, _batch_id -> state end,
        fn state, _batch_id -> state end
      )

    assert BatchScheduler.endpoint_waiting_size(state.batch_scheduler, endpoint_key) == 36
    assert_received {:resume_waiting_batch_connections, ^endpoint_key}
  end

  test "endpoint wakeups do not spin when every batch remains capacity blocked" do
    endpoint_key = :blocked_endpoint
    state = waiting_batches(100, endpoint_key)

    requeue = fn state, batch_id ->
      scheduler =
        BatchScheduler.wait_for_connection(state.batch_scheduler, batch_id, endpoint_key)

      %{state | batch_scheduler: scheduler}
    end

    state =
      CoordinatorBatchRuntime.resume_waiting_endpoint(
        state,
        endpoint_key,
        64,
        requeue,
        requeue
      )

    assert BatchScheduler.endpoint_waiting_size(state.batch_scheduler, endpoint_key) == 100
    refute_received {:resume_waiting_batch_connections, ^endpoint_key}
  end

  defp waiting_batches(count, endpoint_key) do
    Enum.reduce(1..count, %State{}, fn _index, state ->
      batch = %{id: make_ref(), phase: :running}

      scheduler =
        state.batch_scheduler
        |> BatchScheduler.put(batch)
        |> BatchScheduler.wait_for_connection(batch.id, endpoint_key)

      %{state | batch_scheduler: scheduler}
    end)
  end
end
