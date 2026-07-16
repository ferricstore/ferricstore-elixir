defmodule FerricStore.SDK.Native.CoordinatorRequestTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    BatchOperation,
    BatchScheduler,
    CoordinatorRequest,
    CoordinatorRequestRuntime,
    RequestRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "constructs complete control and routed request envelopes" do
    opts = RequestContext.new([], 100)

    assert %{
             kind: :control,
             from: :from,
             opcode: 1,
             key: nil,
             payload: %{},
             opts: ^opts,
             attempt: 0,
             original_reason: nil
           } = CoordinatorRequest.control(:from, 1, %{}, opts)

    assert %{kind: :routed, key: "key", attempt: 0, original_reason: nil} =
             CoordinatorRequest.routed(:from, 2, "key", %{}, opts)
  end

  test "batch operations have one typed and fully initialized state" do
    opts = RequestContext.new([max_group_concurrency: 7], 100)

    assert %BatchOperation{
             from: :from,
             opcode: 3,
             items: ["a"],
             item_count: 1,
             max_concurrency: 7,
             phase: :routing,
             request_tags: request_tags,
             successes: [],
             failures: []
           } = BatchOperation.new(:from, 3, ["a"], 1, & &1, &Map.new/1, opts)

    assert MapSet.size(request_tags) == 0
  end

  test "client state inspection supports compact prepared batches" do
    opts = RequestContext.new([], 100)
    batch = BatchOperation.new_prepared(:from, 3, :mget, 2, & &1, &Map.new/1, opts)
    scheduler = %BatchScheduler{batches: %{batch.id => batch}}

    rendered = inspect(%State{batch_scheduler: scheduler}, limit: :infinity)

    refute rendered =~ "Inspect.Error"
    assert rendered =~ "item_count: 2"
  end

  test "client state inspection redacts nested batch group values" do
    secret = "nested-batch-secret"
    tag = make_ref()

    group = %{
      conn: self(),
      route: %{shard: 1, lane_id: 2},
      indexes: [0],
      items: [{"key", secret}],
      payload: %{"pairs" => [%{"key" => "key", "value" => secret}]}
    }

    request =
      CoordinatorRequest.batch_group(
        make_ref(),
        group,
        tag,
        nil,
        RequestContext.new([], 100)
      )

    registry = RequestRegistry.put(%RequestRegistry{}, tag, request)
    rendered = inspect(%State{request_registry: registry}, limit: :infinity)

    refute rendered =~ secret
    assert rendered =~ "[REDACTED]"
  end

  test "a queued connection success cannot win after the coordinator deadline" do
    tag = make_ref()
    context = RequestContext.new([timeout: 0], 100)

    request =
      :from
      |> CoordinatorRequest.control(1, %{}, context)
      |> CoordinatorRequest.registered(tag, 0, nil, nil)
      |> Map.put(:conn, self())

    state = %State{request_registry: RequestRegistry.put(%RequestRegistry{}, tag, request)}
    test_pid = self()

    callbacks = %{
      ensure_connection: fn state, _endpoint, _key, _waiter -> {:waiting, state} end,
      handle_timeout: fn state, timed_out ->
        send(test_pid, {:timed_out, timed_out})
        {:noreply, state}
      end,
      retry: fn state, _request, result ->
        send(test_pid, {:retried, result})
        {:noreply, state}
      end,
      batch_result: fn state, _request, result ->
        send(test_pid, {:batch_result, result})
        {:noreply, state}
      end,
      reply: fn state, _request, result ->
        send(test_pid, {:replied, result})
        {:noreply, state}
      end,
      remove_waiter: fn state, _key, _tag -> state end,
      cancel_refresh: fn state, _key -> state end,
      resume_wire_slots: & &1
    }

    assert {:noreply, _state} =
             CoordinatorRequestRuntime.handle_response(
               state,
               self(),
               tag,
               {:ok, "late"},
               callbacks
             )

    assert_receive {:timed_out, ^request}
    refute_receive {:replied, _result}
  end
end
