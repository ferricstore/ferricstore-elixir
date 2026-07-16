defmodule FerricStore.Transport.EventDispatcherCallerRuntime do
  @moduledoc false

  alias FerricStore.Transport.{EventDispatcherCallerRegistry, EventDispatcherQueue}

  @spec release(map(), reference() | nil) :: map()
  def release(state, nil), do: state

  def release(state, request_ref) do
    %{state | callers: EventDispatcherCallerRegistry.release(state.callers, request_ref)}
  end

  @spec expire(map(), reference(), reference()) :: map()
  def expire(state, request_ref, monitor) do
    case EventDispatcherCallerRegistry.expire(state.callers, request_ref, monitor) do
      {true, callers} -> EventDispatcherQueue.cancel(%{state | callers: callers}, request_ref)
      {false, callers} -> %{state | callers: callers}
    end
  end

  @spec down(map(), reference()) :: map()
  def down(state, monitor) do
    case EventDispatcherCallerRegistry.down(state.callers, monitor) do
      {nil, callers} ->
        %{state | callers: callers}

      {request_ref, callers} ->
        EventDispatcherQueue.cancel(%{state | callers: callers}, request_ref)
    end
  end

  @spec clear(map()) :: map()
  def clear(state),
    do: %{state | callers: EventDispatcherCallerRegistry.clear(state.callers)}
end
