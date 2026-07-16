defmodule FerricStore.SDK.Native.BatchCoordinatorTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.PreparedMap
  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    BatchCoordinator,
    BatchOperation,
    BatchScheduler,
    CoordinatorBatchRuntime,
    Topology,
    TopologyManager
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "batch preparer startup failures return an error without crashing the coordinator" do
    {:ok, operation_supervisor} =
      DynamicSupervisor.start_link(strategy: :one_for_one, max_children: 0)

    state = %State{operation_supervisor: operation_supervisor}
    context = RequestContext.new([timeout: :infinity], 100)
    from = {self(), make_ref()}
    monitors_before = process_monitor_count()

    assert {:reply, {:error, {:batch_preparer_start_failed, :max_children}}, ^state} =
             BatchCoordinator.dispatch_items(
               state,
               from,
               0x0104,
               ["key"],
               1,
               & &1,
               &%{"keys" => &1},
               context
             )

    assert Process.alive?(operation_supervisor)
    assert process_monitor_count() == monitors_before
    refute_receive {:batch_timeout, _batch_id}, 20
  end

  test "a late batch preparation error is finalized as an absolute deadline timeout" do
    reply_tag = make_ref()
    context = RequestContext.new([timeout: 0], 100)

    batch =
      BatchOperation.new(
        {self(), reply_tag},
        0x0104,
        ["key"],
        1,
        & &1,
        &%{"keys" => &1},
        context
      )

    state = %State{batch_scheduler: BatchScheduler.put(%BatchScheduler{}, batch)}

    ensure_connection = fn _state, _endpoint, _key, _lane, _waiter ->
      flunk("an expired batch must not start connection work")
    end

    assert {:cleanup, _state, {:batch_retry, batch_id}, false} =
             CoordinatorBatchRuntime.finish_preparation(
               state,
               batch,
               {:error, :preparation_failed},
               ensure_connection
             )

    assert batch_id == batch.id
    assert_receive {^reply_tag, {:error, :timeout}}
  end

  test "expired retry preparation does not start a worker" do
    {:ok, operation_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    context = RequestContext.new([timeout: 0], 100)

    batch =
      BatchOperation.new(
        {self(), make_ref()},
        0x0104,
        ["key"],
        1,
        & &1,
        &%{"keys" => &1},
        context
      )

    state = %State{operation_supervisor: operation_supervisor}

    assert {:error, :timeout, ^state} = BatchCoordinator.begin_preparation(state, batch)
    assert DynamicSupervisor.count_children(operation_supervisor).active == 0
  end

  test "stale prepared KV restoration does not run in the coordinator caller" do
    {:ok, operation_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {:ok, topology} = Topology.build(topology_payload())
    topology_manager = TopologyManager.put_topology(%TopologyManager{}, topology)
    item_count = 100_000
    items = List.duplicate("key", item_count)
    indexes = Enum.to_list(0..(item_count - 1))

    payload = %PreparedMap{
      entries: [],
      entry_count: 0,
      byte_size: 5,
      keys: MapSet.new(),
      metadata: %{operation: :mget, items: items},
      reserved: %{}
    }

    context = RequestContext.new([timeout: :infinity], 100)

    prepared = %{
      opcode: 0x0104,
      operation: :mget,
      item_count: item_count,
      topology_version: make_ref(),
      groups: [%{indexes: indexes, payload: payload}],
      opts: context
    }

    state = %State{
      operation_supervisor: operation_supervisor,
      topology_manager: topology_manager
    }

    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:noreply, next_state} =
             BatchCoordinator.dispatch_prepared_items(
               state,
               {self(), make_ref()},
               prepared,
               fn _state, _batch, _groups -> flunk("stale groups must be prepared again") end
             )

    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert after_reductions - before_reductions < 100_000
    assert BatchScheduler.size(next_state.batch_scheduler) == 1
  end

  defp process_monitor_count do
    {:monitors, monitors} = Process.info(self(), :monitors)
    length(monitors)
  end

  defp topology_payload do
    %{
      "route_epoch" => 1,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1_023,
          "shard" => 0,
          "lane_id" => 1,
          "node" => "batch-coordinator-test",
          "host" => "127.0.0.1",
          "native_port" => 6_388
        }
      ]
    }
  end
end
