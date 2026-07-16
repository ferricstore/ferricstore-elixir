defmodule FerricStore.SDK.Native.CoordinatorCallRuntime do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    AdmissionGate,
    BatchCoordinator,
    CoordinatorEventRuntime,
    CoordinatorReply,
    CoordinatorTimers,
    EventSubscriptionCoordinator,
    KVPreparationCoordinator,
    KVPreparedRequest,
    RequestRegistry,
    TopologyManager,
    TopologyRefreshCall,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type callbacks :: %{
          abandon_pending: (State.t(), reference() -> State.t()),
          dispatch_control: (State.t(), term(), non_neg_integer(), term(), RequestContext.t() ->
                               GenServer.on_start()),
          dispatch_routed: (State.t(),
                            term(),
                            non_neg_integer(),
                            term(),
                            term(),
                            RequestContext.t() ->
                              GenServer.on_start()),
          event_runtime: map(),
          start_batch: (State.t(), term(), [map()] -> State.t()),
          start_refresh: (State.t(), term() -> {:noreply, State.t()})
        }

  @spec handle(term(), term(), State.t(), callbacks()) ::
          {:reply, term(), State.t()} | {:noreply, State.t()}
  def handle(:topology, _from, state, _callbacks),
    do: {:reply, TopologyRuntime.current(state), state}

  def handle(:endpoint_snapshot, _from, state, _callbacks),
    do: {:reply, TopologyManager.snapshot(state.topology_manager), state}

  def handle(
        {:admitted_submission, %AdmissionGate{} = gate, request},
        from,
        %{submission_admission: gate} = state,
        callbacks
      ) do
    :ok = AdmissionGate.release(gate)
    handle(request, from, state, callbacks)
  end

  def handle({:cancel_async, owner, ref}, _from, state, callbacks) do
    state =
      case RequestRegistry.fetch_async(state.request_registry, owner, ref) do
        {:ok, tag, _request} -> callbacks.abandon_pending.(state, tag)
        :error -> state
      end

    {:reply, :ok, state}
  end

  def handle({:refresh_topology, %RequestContext{} = context}, from, state, callbacks) do
    TopologyRefreshCall.start(state, from, context, callbacks.start_refresh)
  end

  def handle({:route, key}, _from, state, _callbacks),
    do: {:reply, TopologyManager.route(state.topology_manager, key), state}

  def handle({:event_subscription, action, subscriber, events, context}, from, state, callbacks)
      when action in [:subscribe, :unsubscribe] and is_pid(subscriber) do
    case EventSubscriptionCoordinator.prepare(state, from, action, subscriber, events, context) do
      {:ok, event_call, next_state} ->
        CoordinatorEventRuntime.enqueue(next_state, event_call, callbacks.event_runtime)

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle({:request, opcode, payload, context}, from, state, callbacks) do
    if CoordinatorTimers.expired?(context),
      do: {:reply, {:error, :timeout}, state},
      else: callbacks.dispatch_control.(state, from, opcode, payload, context)
  end

  def handle({:command, opcode, key, payload, context}, from, state, callbacks) do
    if CoordinatorTimers.expired?(context),
      do: {:reply, {:error, :timeout}, state},
      else: callbacks.dispatch_routed.(state, from, opcode, key, payload, context)
  end

  def handle(
        {:command_items, opcode, items, item_count, key_fun, payload_builder, context},
        from,
        state,
        _callbacks
      ) do
    if CoordinatorTimers.expired?(context),
      do: {:reply, {:error, :timeout}, state},
      else:
        BatchCoordinator.dispatch_items(
          state,
          from,
          opcode,
          items,
          item_count,
          key_fun,
          payload_builder,
          context
        )
  end

  def handle({:kv_preparation_admission, item_count, context}, from, state, _callbacks)
      when is_integer(item_count) and item_count >= 0 do
    KVPreparationCoordinator.admit(state, from, item_count, context)
  end

  def handle({:prepared_command_items, %KVPreparedRequest{} = prepared}, from, state, callbacks) do
    KVPreparationCoordinator.dispatch_prepared(state, from, prepared, callbacks.start_batch)
  end

  def handle({:async_submission, request}, _from, state, callbacks) do
    handle_async(request, state, callbacks)
  end

  defp handle_async({:async_request, caller, ref, opcode, payload, context}, state, callbacks) do
    result =
      if CoordinatorTimers.expired?(context),
        do: {:reply, {:error, :timeout}, state},
        else: callbacks.dispatch_control.(state, {:async, caller, ref}, opcode, payload, context)

    CoordinatorReply.admit(result, ref)
  end

  defp handle_async(
         {:async_command, caller, ref, opcode, key, payload, context},
         state,
         callbacks
       ) do
    result =
      if CoordinatorTimers.expired?(context),
        do: {:reply, {:error, :timeout}, state},
        else:
          callbacks.dispatch_routed.(
            state,
            {:async, caller, ref},
            opcode,
            key,
            payload,
            context
          )

    CoordinatorReply.admit(result, ref)
  end
end
