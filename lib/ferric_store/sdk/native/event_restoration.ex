defmodule FerricStore.SDK.Native.EventRestoration do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    EventRequest,
    EventRestore,
    EventSubscriptions,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @default_timeout 5_000

  @type ensure_connection ::
          (State.t(), map(), term() ->
             {:ok, pid(), State.t()}
             | {:waiting, State.t()}
             | {:error, term(), State.t()})

  @type dispatch_connection :: (State.t(), pid(), non_neg_integer(), map() ->
                                  {:noreply, State.t()})

  @spec retry(State.t(), reference(), ensure_connection(), dispatch_connection()) :: State.t()
  def retry(%State{} = state, token, ensure_connection, dispatch_connection) do
    case EventRestore.activate_retry(State.event_restore(state), token) do
      {:ok, restore} ->
        state
        |> State.put_event_restore(restore)
        |> reconnect(ensure_connection, dispatch_connection)

      :stale ->
        state
    end
  end

  @spec maybe_start(State.t(), pid(), dispatch_connection()) :: State.t()
  def maybe_start(%State{} = state, conn, dispatch_connection) when is_pid(conn) do
    events = restore_payload(state)

    cond do
      is_nil(events) ->
        state

      EventRestore.active?(State.event_restore(state)) ->
        state

      not Process.alive?(conn) ->
        state

      true ->
        {token, restore} = EventRestore.begin(State.event_restore(state), conn)
        opts = RequestContext.new([timeout: @default_timeout], @default_timeout)
        request = EventRequest.restore(State.event_subscriptions(state), opts, token)
        state = State.put_event_restore(state, restore)
        {:noreply, state} = dispatch_connection.(state, conn, 0, request)
        state
    end
  end

  @spec complete(State.t(), map(), term()) :: State.t()
  def complete(%State{} = state, %{restore_token: token, conn: conn}, {:ok, _value}) do
    if EventRestore.inflight?(State.event_restore(state), token) do
      state = State.put_event_restore(state, EventRestore.reset(State.event_restore(state)))

      if not State.event_subscriptions_empty?(state) and Process.alive?(conn),
        do: State.put_event_connection(state, conn),
        else: state
    else
      state
    end
  end

  def complete(%State{} = state, %{restore_token: token}, {:error, reason}) do
    restore = State.event_restore(state)

    if EventRestore.inflight?(restore, token) do
      cond do
        State.event_subscriptions_empty?(state) ->
          State.put_event_restore(state, EventRestore.reset(restore))

        State.live_event_connection?(state) ->
          State.put_event_restore(state, EventRestore.reset(restore))

        true ->
          schedule_retry(state, restore.attempt, reason)
      end
    else
      state
    end
  end

  def complete(%State{} = state, _request, _result), do: state

  @spec reconnect(State.t(), ensure_connection(), dispatch_connection()) :: State.t()
  def reconnect(%State{} = state, ensure_connection, dispatch_connection) do
    cond do
      State.event_subscriptions_empty?(state) ->
        state

      State.live_event_connection?(state) ->
        state

      EventRestore.active?(State.event_restore(state)) ->
        state

      true ->
        restore_or_connect(
          state,
          available_connection(state),
          ensure_connection,
          dispatch_connection
        )
    end
  end

  @spec connection_failed(State.t(), term()) :: State.t()
  def connection_failed(%State{} = state, reason) do
    if State.event_subscriptions_empty?(state),
      do: state,
      else: schedule_retry(state, EventRestore.next_attempt(State.event_restore(state)), reason)
  end

  defp restore_payload(state) do
    cond do
      State.event_subscriptions_empty?(state) ->
        nil

      State.live_event_connection?(state) ->
        nil

      true ->
        state
        |> State.event_subscriptions()
        |> EventSubscriptions.desired_events()
        |> EventSubscriptions.wire_payload()
    end
  end

  defp schedule_retry(state, attempt, reason) do
    restore =
      EventRestore.retry(
        State.event_restore(state),
        attempt,
        reason,
        self(),
        EventRequest.restore_backoff(attempt)
      )

    state |> State.put_event_connection(nil) |> State.put_event_restore(restore)
  end

  defp available_connection(state) do
    state.connection_pool
    |> ConnectionPool.connection_values()
    |> Enum.find(&Process.alive?/1)
  end

  defp restore_or_connect(state, conn, _ensure_connection, dispatch_connection)
       when is_pid(conn),
       do: maybe_start(state, conn, dispatch_connection)

  defp restore_or_connect(state, nil, ensure_connection, dispatch_connection) do
    endpoint = TopologyRuntime.control_endpoint(state)
    key = Topology.endpoint_key(endpoint)

    case ensure_connection.(state, endpoint, {:event_reconnect, key}) do
      {:ok, conn, state} -> maybe_start(state, conn, dispatch_connection)
      {:waiting, state} -> state
      {:error, reason, state} -> connection_failed(state, reason)
    end
  end
end
