defmodule FerricStore.SDK.Native.ConnectionCallRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionCancellation,
    ConnectionDrain,
    ConnectionRequest,
    ConnectionTimers,
    FlowControl
  }

  @spec handle(term(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  def handle({:complete_bootstrap, startup}, _from, state) do
    next_state =
      state
      |> FlowControl.apply_server_capabilities(startup)
      |> ConnectionTimers.schedule_heartbeat()

    {:reply, :ok, next_state}
  end

  def handle(:capacity, _from, state) do
    capacity = %{
      max_in_flight: state.max_in_flight,
      max_in_flight_per_lane: state.max_in_flight_per_lane
    }

    {:reply, capacity, state}
  end

  def handle({:request, opcode, payload, lane_id, timeout, deadline}, from, state) do
    case ConnectionRequest.submit(
           state,
           {:call, from},
           opcode,
           payload,
           lane_id,
           timeout,
           deadline
         ) do
      {:ok, next_state} -> {:noreply, next_state}
      {:error, reason, next_state} -> {:reply, {:error, reason}, next_state}
    end
  end

  def handle({:cancel, reply_to, tag}, _from, state) do
    state = ConnectionCancellation.cancel_async_target(state, reply_to, tag)
    {:reply, :ok, ConnectionDrain.maybe_stop(state)}
  end
end
