defmodule FerricStore.SDK.Native.CoordinatorRequestRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionPool,
    CoordinatorRequest,
    CoordinatorTimers,
    EventCall,
    RequestRegistry
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @default_timeout 5_000

  @type callbacks :: %{
          required(:ensure_connection) => (State.t(), term(), term(), term() -> term()),
          required(:handle_timeout) => (State.t(), map() -> {:noreply, State.t()}),
          required(:retry) => (State.t(), map(), term() -> {:noreply, State.t()}),
          required(:batch_result) => (State.t(), map(), term() -> {:noreply, State.t()}),
          required(:reply) => (State.t(), map(), term() -> {:noreply, State.t()}),
          required(:remove_waiter) => (State.t(), term(), reference() -> State.t()),
          required(:cancel_refresh) => (State.t(), term() -> State.t()),
          required(:resume_wire_slots) => (State.t() -> State.t())
        }

  @spec dispatch_connection(State.t(), pid(), non_neg_integer(), map(), callbacks()) ::
          {:noreply, State.t()}
  def dispatch_connection(state, connection, lane_id, request, callbacks) do
    {tag, state} = register(state, request, lane_id)
    dispatch_registered(state, tag, connection, callbacks)
  end

  @spec queue(State.t(), term(), non_neg_integer(), map(), term() | nil, callbacks()) ::
          {:noreply, State.t()}
  def queue(state, endpoint, lane_id, request, connection_key, callbacks) do
    {tag, state} = register(state, request, lane_id)

    case callbacks.ensure_connection.(state, endpoint, connection_key, tag) do
      {:ok, connection, next_state} ->
        dispatch_registered(next_state, tag, connection, callbacks)

      {:waiting, next_state} ->
        {:noreply, next_state}

      {:error, reason, next_state} ->
        fail(next_state, tag, reason, callbacks)
    end
  end

  @spec register(State.t(), map(), non_neg_integer()) :: {reference(), State.t()}
  def register(state, request, lane_id) do
    caller_monitor =
      Map.get(request, :caller_monitor) || CoordinatorRequest.caller_monitor(request)

    tag = RequestRegistry.request_tag(request)
    timer = CoordinatorTimers.pending_request_timer(tag, request.opts)
    request = CoordinatorRequest.registered(request, tag, lane_id, timer, caller_monitor)

    state =
      state |> State.put_pending_request(tag, request) |> track_event_request_tag(request, tag)

    {tag, state}
  end

  @spec dispatch_registered(State.t(), reference(), pid(), callbacks()) ::
          {:noreply, State.t()}
  def dispatch_registered(state, tag, connection, callbacks) do
    case RequestRegistry.fetch(state.request_registry, tag) do
      {:ok, request} ->
        if CoordinatorTimers.expired?(request.opts) do
          fail(state, tag, :timeout, callbacks)
        else
          send_request(state, tag, connection, request)
        end

      :error ->
        {:noreply, state}
    end
  end

  @spec fail(State.t(), reference(), term(), callbacks()) :: {:noreply, State.t()}
  def fail(state, tag, reason, callbacks) do
    result =
      case State.pop_pending_request(state, tag) do
        {nil, state} ->
          {:noreply, state}

        {request, state} ->
          CoordinatorTimers.cancel(request.timer)

          if reason == :timeout,
            do: callbacks.handle_timeout.(state, request),
            else: callbacks.retry.(state, request, reason)
      end

    resume_wire_slots(result, callbacks)
  end

  @spec handle_response(State.t(), pid(), reference(), term(), callbacks()) ::
          {:noreply, State.t()}
  def handle_response(state, connection, tag, result, callbacks) do
    case State.pop_pending_request(state, tag) do
      {nil, state} ->
        {:noreply, state}

      {%{conn: ^connection} = request, state} ->
        CoordinatorTimers.cancel(request.timer)
        complete_response(state, request, result, callbacks)

      {request, state} ->
        {:noreply, State.put_pending_request(state, tag, request)}
    end
  end

  @spec abandon(State.t(), reference(), callbacks()) :: State.t()
  def abandon(state, tag, callbacks) do
    case State.pop_pending_request(state, tag) do
      {nil, state} ->
        state

      {request, state} ->
        CoordinatorTimers.cancel(request.timer)
        CoordinatorTimers.demonitor(request.caller_monitor)
        cancel_connection_request(request, tag)

        state
        |> callbacks.remove_waiter.(Map.get(request, :connection_key), tag)
        |> callbacks.cancel_refresh.({:request_retry, tag})
        |> callbacks.resume_wire_slots.()
    end
  end

  defp send_request(state, tag, connection, request) do
    timeout = CoordinatorTimers.connection_timeout(request.opts, @default_timeout)

    Connection.acknowledged_async_request(
      connection,
      self(),
      tag,
      request.opcode,
      request.payload,
      request.lane_id,
      timeout
    )

    request_registry =
      RequestRegistry.update!(state.request_registry, tag, &Map.put(&1, :conn, connection))

    {:noreply,
     %{
       state
       | request_registry: request_registry,
         connection_pool:
           ConnectionPool.mark_busy(state.connection_pool, connection, request.lane_id)
     }}
  end

  defp track_event_request_tag(state, %{kind: kind, event_call: %{id: event_call_id}}, tag)
       when kind in [:event_subscribe, :event_unsubscribe] do
    case State.event_operation(state) do
      %{id: ^event_call_id} = event_call ->
        State.put_event_operation(state, EventCall.put_request_tag(event_call, tag))

      _other ->
        state
    end
  end

  defp track_event_request_tag(state, _request, _tag), do: state

  defp complete_response(state, %{opts: opts} = request, result, callbacks) do
    if CoordinatorTimers.expired?(opts),
      do: callbacks.handle_timeout.(state, request),
      else: complete_active_response(state, request, result, callbacks)
  end

  defp complete_active_response(state, %{kind: :batch_group} = request, result, callbacks),
    do: callbacks.batch_result.(state, request, result)

  defp complete_active_response(state, %{kind: :event_restore} = request, result, callbacks),
    do: callbacks.reply.(state, request, result)

  defp complete_active_response(state, request, {:error, reason}, callbacks),
    do: callbacks.retry.(state, request, reason)

  defp complete_active_response(state, request, {:ok, _value} = result, callbacks),
    do: callbacks.reply.(state, request, result)

  defp cancel_connection_request(%{conn: connection}, tag) when is_pid(connection),
    do: Connection.cancel_async(connection, self(), tag)

  defp cancel_connection_request(_request, _tag), do: :ok

  defp resume_wire_slots({:noreply, state}, callbacks),
    do: {:noreply, callbacks.resume_wire_slots.(state)}
end
