defmodule FerricStore.SDK.Native.CoordinatorRequestOrchestration do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorEventRuntime,
    CoordinatorReply,
    CoordinatorRequestRuntime,
    CoordinatorRetryRuntime,
    CoordinatorSubmissionRuntime,
    CoordinatorTimers,
    EventRestoration
  }

  def abandon_pending_request(state, tag, callbacks),
    do: CoordinatorRequestRuntime.abandon(state, tag, callbacks)

  def dispatch_control_call(state, from, opcode, payload, context, callbacks) do
    CoordinatorSubmissionRuntime.control(
      state,
      from,
      opcode,
      payload,
      context,
      callbacks
    )
  end

  def dispatch_routed_call(state, from, opcode, key, payload, context, callbacks) do
    CoordinatorSubmissionRuntime.routed(
      state,
      from,
      opcode,
      key,
      payload,
      context,
      callbacks
    )
  end

  def dispatch_connection(state, conn, lane_id, request, callbacks) do
    CoordinatorRequestRuntime.dispatch_connection(
      state,
      conn,
      lane_id,
      request,
      callbacks
    )
  end

  def queue_connection_request(state, endpoint, lane_id, request, connection_key, callbacks) do
    CoordinatorRequestRuntime.queue(
      state,
      endpoint,
      lane_id,
      request,
      connection_key,
      callbacks
    )
  end

  def dispatch_registered_request(state, tag, conn, callbacks) do
    CoordinatorRequestRuntime.dispatch_registered(
      state,
      tag,
      conn,
      callbacks
    )
  end

  def fail_registered_request(state, tag, reason, callbacks) do
    CoordinatorRequestRuntime.fail(
      state,
      tag,
      reason,
      callbacks
    )
  end

  def handle_connection_response(state, conn, tag, result, callbacks) do
    CoordinatorRequestRuntime.handle_response(
      state,
      conn,
      tag,
      result,
      callbacks
    )
  end

  def maybe_retry_completed_request(state, request, reason, callbacks),
    do:
      CoordinatorRetryRuntime.completed(
        state,
        request,
        reason,
        callbacks
      )

  def maybe_retry_initial_dispatch(state, request, reason, callbacks),
    do:
      CoordinatorRetryRuntime.initial(
        state,
        request,
        reason,
        callbacks
      )

  def dispatch_retried_pending_request(state, tag, callbacks),
    do: CoordinatorRetryRuntime.dispatch_pending(state, tag, callbacks)

  def fail_retried_pending_request(state, tag, reason, callbacks),
    do:
      CoordinatorRetryRuntime.fail_pending(
        state,
        tag,
        reason,
        callbacks
      )

  def reply_completed_request(state, %{kind: :event_restore} = request, result, _callbacks) do
    {:noreply, EventRestoration.complete(state, request, result)}
  end

  def reply_completed_request(state, %{kind: kind} = request, result, callbacks)
      when kind in [:event_subscribe, :event_unsubscribe] do
    CoordinatorEventRuntime.complete_request(state, request, result, callbacks)
  end

  def reply_completed_request(state, request, result, _callbacks) do
    CoordinatorTimers.demonitor(Map.get(request, :caller_monitor))
    CoordinatorReply.reply(request.from, result)
    {:noreply, state}
  end

  def handle_pending_timeout(state, %{kind: :batch_group} = request, callbacks),
    do: callbacks.batch_result.(state, request, {:error, :timeout})

  def handle_pending_timeout(state, request, callbacks),
    do: callbacks.reply.(state, request, {:error, :timeout})
end
