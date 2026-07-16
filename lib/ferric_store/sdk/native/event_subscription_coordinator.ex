defmodule FerricStore.SDK.Native.EventSubscriptionCoordinator do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    Admission,
    CoordinatorTimers,
    EventCall
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type result :: {:ok, map(), State.t()} | {:error, term()}

  @spec prepare(
          State.t(),
          GenServer.from(),
          :subscribe | :unsubscribe,
          pid(),
          [term()],
          RequestContext.t()
        ) ::
          result()
  def prepare(state, from, action, subscriber, events, %RequestContext{} = context)
      when action in [:subscribe, :unsubscribe] and is_pid(subscriber) do
    cond do
      CoordinatorTimers.expired?(context) ->
        {:error, :timeout}

      Admission.full?(state) ->
        {:error, :client_backpressure}

      true ->
        prepare_admitted(state, from, action, subscriber, events, context)
    end
  end

  defp prepare_admitted(state, from, :subscribe, subscriber, events, context) do
    case State.reserve_event_subscriber(state, subscriber, state.limits.event_subscribers) do
      {:ok, state} ->
        event_call =
          :subscribe
          |> EventCall.new(subscriber, events, context, from)
          |> EventCall.reserve_subscriber()

        put_call_monitor(state, event_call)

      :full ->
        {:error, :event_subscriber_backpressure}
    end
  end

  defp prepare_admitted(state, from, :unsubscribe, subscriber, events, context) do
    event_call = EventCall.new(:unsubscribe, subscriber, events, context, from)
    put_call_monitor(state, event_call)
  end

  defp put_call_monitor(state, event_call) do
    state =
      State.put_lifecycle_monitor(
        state,
        event_call.caller_monitor,
        {:event_call, event_call.id}
      )

    {:ok, event_call, state}
  end
end
