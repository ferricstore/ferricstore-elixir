defmodule FerricStore.SDK.Native.CoordinatorStateTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{Coordinator, LifecycleRegistry, RequestRegistry}

  test "pending request insertion and removal update lifecycle ownership atomically" do
    tag = make_ref()
    monitor = make_ref()
    request = %{kind: :control, caller_monitor: monitor, conn: nil}

    state = Coordinator.State.put_pending_request(%Coordinator.State{}, tag, request)

    assert RequestRegistry.get(state.request_registry, tag) == request
    assert LifecycleRegistry.get(state.lifecycle_registry, monitor) == {:pending_request, tag}

    assert {^request, state} = Coordinator.State.pop_pending_request(state, tag)
    assert RequestRegistry.get(state.request_registry, tag) == nil
    assert LifecycleRegistry.get(state.lifecycle_registry, monitor) == nil
  end

  test "batch group admission changes with request ownership" do
    tag = make_ref()
    request = %{kind: :batch_group, conn: nil}

    state = Coordinator.State.put_pending_request(%Coordinator.State{}, tag, request)
    assert state.admission.batch_groups == 1

    assert {^request, state} = Coordinator.State.pop_pending_request(state, tag)
    assert state.admission.batch_groups == 0
  end

  test "replacing a pending tag releases the previous request ownership" do
    tag = make_ref()
    first_monitor = make_ref()
    replacement_monitor = make_ref()

    state =
      Coordinator.State.put_pending_request(%Coordinator.State{}, tag, %{
        kind: :batch_group,
        caller_monitor: first_monitor,
        conn: nil
      })

    state =
      Coordinator.State.put_pending_request(state, tag, %{
        kind: :control,
        caller_monitor: replacement_monitor,
        conn: nil
      })

    assert LifecycleRegistry.get(state.lifecycle_registry, first_monitor) == nil

    assert LifecycleRegistry.get(state.lifecycle_registry, replacement_monitor) ==
             {:pending_request, tag}

    assert state.admission.batch_groups == 0
  end
end
