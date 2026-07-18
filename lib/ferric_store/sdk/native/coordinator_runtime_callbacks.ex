defmodule FerricStore.SDK.Native.CoordinatorRuntimeCallbacks do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    CoordinatorConnectionOrchestrator,
    CoordinatorRequest,
    EventRestoration
  }

  alias FerricStore.SDK.Native.CoordinatorBatchOrchestration, as: Batch
  alias FerricStore.SDK.Native.CoordinatorLifecycleOrchestration, as: Lifecycle
  alias FerricStore.SDK.Native.CoordinatorRequestOrchestration, as: Request

  @event_runtime %{
    dispatch_connection: &__MODULE__.dispatch_connection/4,
    queue_connection_request: &__MODULE__.queue_connection_request/4,
    remove_connection_waiter: &__MODULE__.remove_connection_waiter/3,
    reconnect_event_connection: &__MODULE__.reconnect_event_connection/1,
    resume_waiting_wire_slots: &__MODULE__.resume_waiting_wire_slots/1
  }

  @topology_refresh %{
    dispatch_request_retry: &__MODULE__.dispatch_retried_pending_request/2,
    fail_request_retry: &__MODULE__.fail_retried_pending_request/3,
    maybe_start_event_restore: &__MODULE__.maybe_start_event_restore/2
  }

  @batch %{
    cancel_refresh: &__MODULE__.cancel_refresh_waiter/2,
    ensure_connection: &CoordinatorConnectionOrchestrator.ensure_batch/5,
    pump_warm: &CoordinatorConnectionOrchestrator.pump_warm/1,
    start_refresh: &__MODULE__.start_topology_refresh/2
  }

  @request %{
    ensure_connection: &__MODULE__.ensure_connection_async/4,
    handle_timeout: &__MODULE__.handle_pending_timeout/2,
    retry: &__MODULE__.maybe_retry_completed_request/3,
    batch_result: &__MODULE__.handle_group_result/3,
    reply: &__MODULE__.reply_completed_request/3,
    remove_waiter: &__MODULE__.remove_connection_waiter/3,
    cancel_refresh: &__MODULE__.cancel_refresh_waiter/2,
    resume_wire_slots: &__MODULE__.resume_waiting_wire_slots/1
  }

  @retry %{
    default_lane: &CoordinatorRequest.default_lane_id/1,
    start_refresh: &__MODULE__.start_topology_refresh/2,
    ensure_connection: &__MODULE__.ensure_connection_async/4,
    reply_completed: &__MODULE__.reply_completed_request/3,
    resume_wire_slots: &__MODULE__.resume_waiting_wire_slots/1,
    request_runtime: @request
  }

  @submission %{
    queue: &__MODULE__.queue_connection_request/5,
    retry: &__MODULE__.maybe_retry_initial_dispatch/3
  }

  @call %{
    abandon_pending: &__MODULE__.abandon_pending_request/2,
    dispatch_control: &__MODULE__.dispatch_control_call/5,
    dispatch_routed: &__MODULE__.dispatch_routed_call/6,
    event_runtime: @event_runtime,
    start_batch: &__MODULE__.start/3,
    start_refresh: &__MODULE__.start_topology_refresh/2
  }

  @connection %{
    dispatch_registered: &__MODULE__.dispatch_registered_request/3,
    event_connection_failed: &EventRestoration.connection_failed/2,
    fail_batch: &__MODULE__.fail_connection/4,
    fail_registered: &__MODULE__.fail_registered_request/3,
    handle_response: &__MODULE__.handle_connection_response/4,
    pump_warm: &CoordinatorConnectionOrchestrator.pump_warm/1,
    reconnect_event: &__MODULE__.reconnect_event_connection/1,
    resume_batch: &__MODULE__.resume_connection/4,
    resume_waiting: &__MODULE__.resume_waiting_connections/1,
    resume_waiting_endpoint: &__MODULE__.resume_waiting_endpoint/2,
    resume_wire_slots: &__MODULE__.resume_waiting_wire_slots/1,
    start_event_restore: &__MODULE__.maybe_start_event_restore/2
  }

  @lifecycle %{
    abandon_refresh_waiter: &__MODULE__.abandon_refresh_waiter/2,
    abandon_pending_request: &__MODULE__.abandon_pending_request/2,
    abandon_batch: &__MODULE__.abandon/2,
    fail_batch_preparer: &__MODULE__.fail_preparer/3,
    connection_started: &__MODULE__.handle_connection_started/3,
    connection_down: &__MODULE__.handle_connection_down/3,
    subscriber_down: &__MODULE__.handle_subscriber_down/3,
    event_runtime: @event_runtime,
    topology_refresh: @topology_refresh
  }

  @server_event %{
    reconnect_event_connection: &__MODULE__.reconnect_event_connection/1,
    refresh_topology: &__MODULE__.refresh_topology_event/1,
    retire_connection: &CoordinatorConnectionOrchestrator.retire/2
  }

  for {name, arguments} <- [
        abandon: [:state, :id],
        fail_connection: [:state, :id, :group, :reason],
        fail_preparer: [:state, :id, :reason],
        handle_group_result: [:state, :request, :result],
        resume_connection: [:state, :id, :group, :conn],
        resume_waiting_connections: [:state],
        resume_waiting_endpoint: [:state, :key],
        resume_waiting_wire_slots: [:state],
        start: [:state, :batch, :groups]
      ] do
    variables = Enum.map(arguments, &Macro.var(&1, nil))

    def unquote(name)(unquote_splicing(variables)),
      do: Batch.unquote(name)(unquote_splicing(variables), @batch)
  end

  for {name, arguments} <- [
        abandon_pending_request: [:state, :tag],
        dispatch_connection: [:state, :conn, :lane, :request],
        dispatch_registered_request: [:state, :tag, :conn],
        fail_registered_request: [:state, :tag, :reason],
        handle_connection_response: [:state, :conn, :tag, :result],
        handle_pending_timeout: [:state, :request]
      ] do
    variables = Enum.map(arguments, &Macro.var(&1, nil))

    def unquote(name)(unquote_splicing(variables)),
      do: Request.unquote(name)(unquote_splicing(variables), @request)
  end

  for {name, arguments} <- [
        dispatch_retried_pending_request: [:state, :tag],
        fail_retried_pending_request: [:state, :tag, :reason],
        maybe_retry_completed_request: [:state, :request, :reason],
        maybe_retry_initial_dispatch: [:state, :request, :reason],
        resume_retried_pending_request: [:state, :tag]
      ] do
    variables = Enum.map(arguments, &Macro.var(&1, nil))

    def unquote(name)(unquote_splicing(variables)),
      do: Request.unquote(name)(unquote_splicing(variables), @retry)
  end

  defdelegate abandon_refresh_waiter(state, monitor), to: Lifecycle
  defdelegate cancel_refresh_waiter(state, key), to: Lifecycle
  defdelegate ensure_connection_async(state, endpoint, connection_key, waiter), to: Lifecycle

  def dispatch_control_call(state, from, opcode, payload, context),
    do: Request.dispatch_control_call(state, from, opcode, payload, context, @submission)

  def dispatch_routed_call(state, from, opcode, key, payload, context),
    do: Request.dispatch_routed_call(state, from, opcode, key, payload, context, @submission)

  def queue_connection_request(state, endpoint, lane, request),
    do: Request.queue_connection_request(state, endpoint, lane, request, nil, @request)

  def queue_connection_request(state, endpoint, lane, request, key),
    do: Request.queue_connection_request(state, endpoint, lane, request, key, @request)

  def reply_completed_request(state, request, result),
    do: Request.reply_completed_request(state, request, result, @event_runtime)

  def handle_connection_down(state, conn, reason),
    do: Lifecycle.handle_connection_down(state, conn, reason, @connection)

  def handle_connection_started(state, attempt, result),
    do: Lifecycle.handle_connection_started(state, attempt, result, @connection)

  def handle_subscriber_down(state, monitor, subscriber),
    do: Lifecycle.handle_subscriber_down(state, monitor, subscriber, @event_runtime)

  def maybe_start_event_restore(state, conn),
    do: Lifecycle.maybe_start_event_restore(state, conn, @event_runtime)

  def reconnect_event_connection(state),
    do: Lifecycle.reconnect_event_connection(state, @event_runtime)

  def refresh_topology_event(state),
    do: Lifecycle.refresh_topology_event(state, @topology_refresh)

  def remove_connection_waiter(state, key, tag),
    do: Lifecycle.remove_connection_waiter(state, key, tag, @connection)

  def start_topology_refresh(state, waiter),
    do: Lifecycle.start_topology_refresh(state, waiter, @topology_refresh)

  def batch, do: @batch
  def call, do: @call
  def connection, do: @connection
  def event_runtime, do: @event_runtime
  def lifecycle, do: @lifecycle
  def request, do: @request
  def retry, do: @retry
  def server_event, do: @server_event
  def submission, do: @submission
  def topology_refresh, do: @topology_refresh
end
