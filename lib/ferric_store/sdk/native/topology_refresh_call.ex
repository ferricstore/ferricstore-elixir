defmodule FerricStore.SDK.Native.TopologyRefreshCall do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    Admission,
    CoordinatorTimers,
    LifecycleRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec start(
          State.t(),
          GenServer.from(),
          RequestContext.t(),
          (State.t(), term() -> {:noreply, State.t()})
        ) :: {:reply, term(), State.t()} | {:noreply, State.t()}
  def start(state, from, context, start_refresh) when is_function(start_refresh, 2) do
    cond do
      CoordinatorTimers.expired?(context) ->
        {:reply, {:error, :timeout}, state}

      Admission.full?(state) ->
        {:reply, {:error, :client_backpressure}, state}

      true ->
        monitor = Process.monitor(elem(from, 0))
        timer = CoordinatorTimers.refresh_waiter_timer(monitor, from, context)
        waiter = {:refresh_call, from, monitor, timer, context}

        state =
          state
          |> State.put_lifecycle_monitor(monitor, {:refresh_waiter, monitor})
          |> State.adjust_refresh_calls(1)

        start_refresh.(state, waiter)
    end
  end

  @spec timeout(State.t(), reference(), GenServer.from(), (State.t(), term() -> State.t())) ::
          {:noreply, State.t()}
  def timeout(state, monitor, from, cancel_refresh) when is_reference(monitor) do
    owner = {:refresh_waiter, monitor}

    if LifecycleRegistry.get(state.lifecycle_registry, monitor) == owner do
      Process.demonitor(monitor, [:flush])
      GenServer.reply(from, {:error, :timeout})

      state =
        state
        |> State.delete_lifecycle_monitor(monitor, owner)
        |> cancel_refresh.({:refresh_call, monitor})
        |> State.adjust_refresh_calls(-1)

      {:noreply, state}
    else
      {:noreply, state}
    end
  end
end
