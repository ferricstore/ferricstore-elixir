defmodule FerricStore.SDK.Native.Coordinator.StateLifecycle do
  @moduledoc false

  alias FerricStore.SDK.Native.{Admission, LifecycleRegistry, TopologyManager}

  def put_monitor(state, monitor, owner) when is_reference(monitor),
    do: %{
      state
      | lifecycle_registry: LifecycleRegistry.put(state.lifecycle_registry, monitor, owner)
    }

  def put_monitor(state, _monitor, _owner), do: state

  def delete_monitor(state, monitor, owner) when is_reference(monitor),
    do: %{
      state
      | lifecycle_registry: LifecycleRegistry.delete(state.lifecycle_registry, monitor, owner)
    }

  def delete_monitor(state, _monitor, _owner), do: state

  def adjust_batch_groups(state, delta) when is_integer(delta),
    do: %{state | admission: Admission.adjust_batch_groups(state.admission, delta)}

  def adjust_refresh_calls(state, delta) when is_integer(delta),
    do: %{
      state
      | topology_manager: TopologyManager.adjust_refresh_calls(state.topology_manager, delta)
    }
end
