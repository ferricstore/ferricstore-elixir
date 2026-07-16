defmodule FerricStore.SDK.Native.CoordinatorEventCompletion do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorEventConnectionRuntime,
    CoordinatorEventOperationRuntime,
    CoordinatorEventRuntime,
    EventCommit
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec complete_request(State.t(), map(), term(), CoordinatorEventRuntime.callbacks()) ::
          {:noreply, State.t()}
  def complete_request(state, %{kind: kind} = request, result, callbacks)
      when kind in [:event_subscribe, :event_unsubscribe] do
    state = commit(state, request, result)
    state = maybe_reset_uncertain_connection(state, request, result, callbacks)
    CoordinatorEventOperationRuntime.finish(state, request.event_call, result, callbacks)
  end

  defp commit(state, %{kind: :event_subscribe} = request, {:ok, _value}) do
    EventCommit.subscribe(
      state,
      request.event_call.subscriber,
      request.event_changes,
      request.conn
    )
  end

  defp commit(state, %{kind: :event_unsubscribe} = request, {:ok, _value}) do
    EventCommit.unsubscribe(state, request.event_call.subscriber, request.event_changes)
  end

  defp commit(
         state,
         %{kind: :event_unsubscribe, event_call: %{subscriber_down: true}} = request,
         _result
       ) do
    EventCommit.unsubscribe(state, request.event_call.subscriber, request.event_changes)
  end

  defp commit(state, _request, _result), do: state

  defp maybe_reset_uncertain_connection(
         state,
         %{kind: :event_unsubscribe, event_call: %{subscriber_down: true}} = request,
         {:error, _reason},
         callbacks
       ) do
    CoordinatorEventConnectionRuntime.reset(
      state,
      request,
      :event_subscriber_cleanup_failed,
      callbacks
    )
  end

  defp maybe_reset_uncertain_connection(state, request, {:error, :timeout}, callbacks) do
    CoordinatorEventConnectionRuntime.reset(state, request, :event_request_timeout, callbacks)
  end

  defp maybe_reset_uncertain_connection(state, _request, _result, _callbacks), do: state
end
