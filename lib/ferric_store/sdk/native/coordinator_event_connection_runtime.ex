defmodule FerricStore.SDK.Native.CoordinatorEventConnectionRuntime do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionLifecycle,
    CoordinatorEventRuntime,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec queue_request(State.t(), map(), CoordinatorEventRuntime.callbacks()) ::
          {:noreply, State.t()}
  def queue_request(state, request, callbacks) do
    connection = State.event_connection(state)

    if is_pid(connection) and Process.alive?(connection) do
      callbacks.dispatch_connection.(state, connection, 0, request)
    else
      state = State.put_event_connection(state, nil)

      endpoint =
        RequestContext.option(request.opts, :endpoint) || TopologyRuntime.control_endpoint(state)

      callbacks.queue_connection_request.(state, endpoint, 0, request)
    end
  end

  @spec reset(State.t(), map(), term(), CoordinatorEventRuntime.callbacks()) :: State.t()
  def reset(state, request, reason, callbacks) do
    case Map.get(request, :conn) do
      conn when is_pid(conn) ->
        state = %{
          state
          | connection_pool: ConnectionLifecycle.remove(state.connection_pool, conn)
        }

        state = State.clear_event_connection(state, conn)
        Connection.abort(conn, reason)
        callbacks.reconnect_event_connection.(state)

      _other ->
        state
    end
  end
end
