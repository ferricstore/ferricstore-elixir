defmodule FerricStore.SDK.Native.CoordinatorEventRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorEventCancellation,
    CoordinatorEventCompletion,
    CoordinatorEventQueueRuntime,
    EventCall
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type callbacks :: %{
          required(:dispatch_connection) => (State.t(), pid(), non_neg_integer(), map() ->
                                               {:noreply, State.t()}),
          required(:queue_connection_request) => (State.t(), map(), non_neg_integer(), map() ->
                                                    {:noreply, State.t()}),
          required(:remove_connection_waiter) => (State.t(), term(), term() -> State.t()),
          required(:reconnect_event_connection) => (State.t() -> State.t()),
          required(:resume_waiting_wire_slots) => (State.t() -> State.t())
        }

  @spec enqueue(State.t(), EventCall.t(), callbacks()) :: {:noreply, State.t()}
  defdelegate enqueue(state, event_call, callbacks), to: CoordinatorEventQueueRuntime

  @spec timeout_queued(State.t(), reference()) :: {:noreply, State.t()}
  defdelegate timeout_queued(state, event_call_id), to: CoordinatorEventQueueRuntime, as: :timeout

  @spec abandon(State.t(), reference(), callbacks()) :: State.t()
  defdelegate abandon(state, event_call_id, callbacks), to: CoordinatorEventCancellation

  @spec complete_request(State.t(), map(), term(), callbacks()) :: {:noreply, State.t()}
  defdelegate complete_request(state, request, result, callbacks),
    to: CoordinatorEventCompletion
end
