defmodule FerricStore.Transport.EventDispatcherAdmission do
  @moduledoc false

  alias FerricStore.Transport.{
    EventDispatcherCallerRegistry,
    EventDispatcherCallerRuntime,
    EventDispatcherQueue,
    EventDispatcherShutdown
  }

  @spec prepare(map(), pid(), reference(), term()) ::
          {map(), :ok | :dropped | :dropped_oldest}
  def prepare(state, caller, request_ref, event) do
    if EventDispatcherShutdown.stopping?(state),
      do: {state, :dropped},
      else: prepare_active(state, caller, request_ref, event)
  end

  defp prepare_active(state, caller, request_ref, event) do
    case EventDispatcherQueue.prepare(state, request_ref, event) do
      {state, :dropped, nil} ->
        {state, :dropped}

      {state, result, evicted_request_ref} ->
        state = EventDispatcherCallerRuntime.release(state, evicted_request_ref)

        callers =
          EventDispatcherCallerRegistry.track(
            state.callers,
            request_ref,
            caller,
            state.commit_timeout
          )

        {%{state | callers: callers}, result}
    end
  end
end
