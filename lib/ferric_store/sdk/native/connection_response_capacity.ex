defmodule FerricStore.SDK.Native.ConnectionResponseCapacity do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionEventHandler, FlowControl}

  def apply_window_update(state, opcode, result) do
    window_limits = FlowControl.response_window_limits(opcode, result)
    apply_window_limits(state, window_limits)
  end

  def apply_window_limits(state, window_limits) do
    previous = capacity_profile(state)
    next_state = FlowControl.apply_window_limits(state, window_limits)
    notify_capacity_change(next_state, previous)
    next_state
  end

  defp notify_capacity_change(state, previous) do
    capacity = capacity_profile(state)

    if capacity != previous do
      ConnectionEventHandler.capacity_changed(state.event_handler, self(), capacity)
    end
  end

  defp capacity_profile(state),
    do: %{
      max_in_flight: state.max_in_flight,
      max_in_flight_per_lane: state.max_in_flight_per_lane
    }
end
