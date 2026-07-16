defmodule FerricStore.SDK.Native.CoordinatorConnectionAcquisition do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    CoordinatorConnectionAttempt,
    CoordinatorConnectionRuntime,
    EndpointPolicy,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type ensure_result ::
          {:ok, pid(), State.t()}
          | {:waiting, State.t()}
          | {:capacity, State.t()}
          | {:error, term(), State.t()}

  @type completion_callbacks :: %{
          fail_waiter: (State.t(), term(), term() -> State.t()),
          maybe_restore: (State.t(), pid() -> State.t()),
          pump_warm: (State.t() -> State.t()),
          resume_waiter: (State.t(), term(), pid() -> State.t()),
          resume_waiting: (State.t() -> State.t()),
          resume_waiting_endpoint: (State.t(), term() -> State.t())
        }

  @spec ensure(State.t(), map(), term() | nil, term()) :: ensure_result()
  def ensure(state, endpoint, nil, waiter) do
    case EndpointPolicy.normalize(endpoint) do
      {:ok, endpoint} ->
        endpoint = TopologyRuntime.endpoint_defaults(endpoint, state)
        checkout(state, endpoint, Topology.endpoint_key(endpoint), waiter)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  def ensure(state, endpoint, connection_key, waiter) do
    checkout(
      state,
      TopologyRuntime.endpoint_defaults(endpoint, state),
      connection_key,
      waiter
    )
  end

  @spec ensure_batch(State.t(), map(), term(), non_neg_integer(), term()) :: ensure_result()
  def ensure_batch(state, endpoint, connection_key, lane_id, waiter) do
    endpoint = TopologyRuntime.endpoint_defaults(endpoint, state)
    state = CoordinatorConnectionRuntime.put_waiter_connection_key(state, waiter, connection_key)

    case ConnectionPool.checkout_capacity(
           state.connection_pool,
           connection_key,
           lane_id,
           waiter
         ) do
      {:ready, connection, pool} ->
        {:ok, connection, %{state | connection_pool: pool}}

      {:waiting, pool} ->
        {:waiting, %{state | connection_pool: pool}}

      {:start, pool} ->
        CoordinatorConnectionAttempt.start(
          %{state | connection_pool: pool},
          connection_key,
          endpoint,
          waiter
        )

      {:capacity, pool} ->
        {:capacity, %{state | connection_pool: pool}}

      {:error, reason, pool} ->
        {:error, reason, %{state | connection_pool: pool}}
    end
  end

  @spec warm(State.t()) :: State.t()
  def warm(%{warmup: %{enabled: true}} = state) do
    queued =
      Enum.reject(TopologyRuntime.current(state).endpoints, fn {key, _endpoint} ->
        ConnectionPool.contains?(state.connection_pool, key)
      end)

    state
    |> put_warm_queue(:queue.from_list(queued))
    |> pump_warm()
  end

  def warm(state), do: state

  @spec pump_warm(State.t()) :: State.t()
  def pump_warm(state),
    do:
      if(ConnectionPool.full?(state.connection_pool),
        do: state,
        else: continue_warm(state, :queue.out(state.warmup.queue))
      )

  @spec complete(State.t(), map(), term(), completion_callbacks()) :: {:noreply, State.t()}
  def complete(state, attempt, {:ok, connection, capacity}, callbacks)
      when is_pid(connection) do
    if Process.alive?(connection) do
      complete_live_connection(state, attempt, connection, capacity, callbacks)
    else
      complete(state, attempt, {:error, :closed}, callbacks)
    end
  end

  def complete(state, attempt, {:error, reason}, callbacks) do
    state =
      Enum.reduce(attempt.waiters, state, fn waiter, state ->
        callbacks.fail_waiter.(state, waiter, reason)
      end)

    state = callbacks.resume_waiting.(state)
    {:noreply, callbacks.pump_warm.(state)}
  end

  @spec remove_waiter(State.t(), term() | nil, term(), completion_callbacks()) :: State.t()
  def remove_waiter(state, key, tag, callbacks) do
    case CoordinatorConnectionRuntime.remove_waiter(state, key, tag) do
      {:ok, state} ->
        state

      {:capacity_released, state} ->
        state = callbacks.resume_waiting.(state)
        callbacks.pump_warm.(state)
    end
  end

  defp checkout(state, endpoint, key, waiter) do
    state = CoordinatorConnectionRuntime.put_waiter_connection_key(state, waiter, key)

    case ConnectionPool.checkout(state.connection_pool, key, waiter) do
      {:ready, connection, pool} ->
        {:ok, connection, %{state | connection_pool: pool}}

      {:waiting, pool} ->
        {:waiting, %{state | connection_pool: pool}}

      {:start, pool} ->
        CoordinatorConnectionAttempt.start(
          %{state | connection_pool: pool},
          key,
          endpoint,
          waiter
        )

      {:error, reason, pool} ->
        {:error, reason, %{state | connection_pool: pool}}
    end
  end

  defp continue_warm(state, {:empty, _queue}), do: state

  defp continue_warm(state, {{:value, {key, endpoint}}, queue}) do
    state = put_warm_queue(state, queue)

    state =
      case ensure(state, endpoint, key, {:warm_connection, key}) do
        {:ok, _connection, state} -> state
        {:waiting, state} -> state
        {:error, _reason, state} -> state
      end

    pump_warm(state)
  end

  defp put_warm_queue(state, queue), do: %{state | warmup: %{state.warmup | queue: queue}}

  defp complete_live_connection(state, attempt, connection, capacity, callbacks) do
    state = CoordinatorConnectionRuntime.track(state, attempt.key, connection, capacity)

    if ConnectionPool.connection?(state.connection_pool, connection) do
      resume_waiters(state, attempt, connection, callbacks)
    else
      complete(state, attempt, {:error, :connection_backpressure}, callbacks)
    end
  end

  defp resume_waiters(state, attempt, connection, callbacks) do
    state = callbacks.maybe_restore.(state, connection)

    state =
      Enum.reduce(attempt.waiters, state, fn waiter, state ->
        callbacks.resume_waiter.(state, waiter, connection)
      end)

    state = callbacks.resume_waiting_endpoint.(state, attempt.key)
    state = callbacks.resume_waiting.(state)
    {:noreply, callbacks.pump_warm.(state)}
  end
end
