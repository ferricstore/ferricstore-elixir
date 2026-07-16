defmodule FerricStore.Transport.EventDispatcherShutdown do
  @moduledoc false

  alias FerricStore.Transport.{EventDispatcherProtocol, EventDispatcherWorker}
  @spec add_waiter(map(), pid(), reference()) :: map()
  def add_waiter(state, caller, request_ref)
      when is_pid(caller) and is_reference(request_ref) do
    %{state | stopping: MapSet.put(state.stopping, {caller, request_ref})}
  end

  @spec stopping?(map()) :: boolean()
  def stopping?(state), do: MapSet.size(state.stopping) > 0

  @spec finish(map()) :: :ok
  def finish(state) do
    EventDispatcherWorker.stop(state.worker)

    Enum.each(state.stopping, fn {caller, request_ref} ->
      send(caller, {EventDispatcherProtocol, :stopped, request_ref})
    end)

    :ok
  end
end
