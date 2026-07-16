defmodule FerricStore.SDK.Native.LifecycleRegistryTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.LifecycleRegistry

  test "monitor ownership is typed and removed in one lookup" do
    request_monitor = make_ref()
    connection_monitor = make_ref()
    request_tag = make_ref()
    connection = self()

    registry =
      LifecycleRegistry.new()
      |> LifecycleRegistry.put(request_monitor, {:pending_request, request_tag})
      |> LifecycleRegistry.put(connection_monitor, {:connection, connection})

    assert LifecycleRegistry.get(registry, request_monitor) ==
             {:pending_request, request_tag}

    assert {{:connection, ^connection}, registry} =
             LifecycleRegistry.pop(registry, connection_monitor)

    assert LifecycleRegistry.get(registry, connection_monitor) == nil
    assert LifecycleRegistry.size(registry) == 1
  end

  test "conditional deletion cannot remove a monitor owned by another operation" do
    monitor = make_ref()
    request_tag = make_ref()

    registry =
      LifecycleRegistry.new()
      |> LifecycleRegistry.put(monitor, {:pending_request, request_tag})
      |> LifecycleRegistry.delete(monitor, {:batch, make_ref()})

    assert LifecycleRegistry.get(registry, monitor) ==
             {:pending_request, request_tag}

    assert LifecycleRegistry.empty?(
             LifecycleRegistry.delete(registry, monitor, {:pending_request, request_tag})
           )
  end

  test "invalid monitor keys and untyped owners are rejected" do
    registry = LifecycleRegistry.new()

    assert_raise FunctionClauseError, fn ->
      # Deliberately bypass static type analysis to exercise the runtime boundary.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      apply(LifecycleRegistry, :put, [registry, self(), {:connection, self()}])
    end

    assert_raise FunctionClauseError, fn ->
      # Deliberately bypass static type analysis to exercise the runtime boundary.
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      apply(LifecycleRegistry, :put, [registry, make_ref(), {:arbitrary, :owner}])
    end
  end
end
