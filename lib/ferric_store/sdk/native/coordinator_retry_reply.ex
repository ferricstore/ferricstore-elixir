defmodule FerricStore.SDK.Native.CoordinatorRetryReply do
  @moduledoc false

  alias FerricStore.SDK.Native.{CoordinatorReply, CoordinatorTimers}

  def run(state, %{kind: kind} = request, result, callbacks)
      when kind in [:event_subscribe, :event_unsubscribe],
      do: callbacks.reply_completed.(state, request, result)

  def run(state, request, result, _callbacks) do
    CoordinatorTimers.demonitor(Map.get(request, :caller_monitor))
    CoordinatorReply.reply(request.from, result)
    {:noreply, state}
  end
end
