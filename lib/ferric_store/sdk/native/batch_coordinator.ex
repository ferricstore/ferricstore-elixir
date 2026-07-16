defmodule FerricStore.SDK.Native.BatchCoordinator do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    Admission,
    BatchOperation,
    BatchPreparationStarter,
    CoordinatorTimers,
    KVBatchPreparer,
    KVBatchRestorer,
    KVPayloadPreparer,
    TopologyManager
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec admit(State.t(), non_neg_integer()) :: :ok | {:error, term()}
  def admit(state, item_count) do
    cond do
      item_count > state.limits.batch_items ->
        {:error, {:batch_too_large, %{items: item_count, limit: state.limits.batch_items}}}

      Admission.full?(state) ->
        {:error, :client_backpressure}

      true ->
        :ok
    end
  end

  @spec dispatch_items(
          State.t(),
          GenServer.from(),
          non_neg_integer(),
          list() | map(),
          non_neg_integer(),
          function(),
          function(),
          RequestContext.t()
        ) :: {:reply, term(), State.t()} | {:noreply, State.t()}
  def dispatch_items(state, _from, _opcode, [], 0, _key_fun, _payload_builder, %RequestContext{}) do
    {:reply, {:ok, []}, state}
  end

  def dispatch_items(state, from, opcode, items, item_count, key_fun, payload_builder, opts) do
    %RequestContext{} = opts

    case admit(state, item_count) do
      :ok ->
        batch =
          BatchOperation.new(
            from,
            opcode,
            items,
            item_count,
            key_fun,
            payload_builder,
            opts
          )

        preparation_result(begin_preparation(state, batch))

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @spec dispatch_prepared_items(
          State.t(),
          GenServer.from(),
          map(),
          (State.t(), BatchOperation.t(), [map()] -> State.t())
        ) :: {:reply, term(), State.t()} | {:noreply, State.t()}
  def dispatch_prepared_items(
        state,
        from,
        %{item_count: item_count} = prepared,
        start_batch
      ) do
    case admit(state, item_count) do
      :ok ->
        dispatch_admitted_prepared_items(state, from, prepared, start_batch)

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @spec dispatch_reserved_prepared_items(
          State.t(),
          GenServer.from(),
          map(),
          (State.t(), BatchOperation.t(), [map()] -> State.t())
        ) :: {:reply, term(), State.t()} | {:noreply, State.t()}
  def dispatch_reserved_prepared_items(state, from, prepared, start_batch),
    do: dispatch_admitted_prepared_items(state, from, prepared, start_batch)

  defp dispatch_admitted_prepared_items(
         state,
         from,
         %{
           opcode: opcode,
           operation: operation,
           item_count: item_count,
           topology_version: topology_version,
           groups: groups,
           opts: opts
         },
         start_batch
       ) do
    {item_router, payload_builder} = KVBatchPreparer.preparation_callbacks(operation)
    group_preparer = &KVPayloadPreparer.prepare(&1, operation, opts)

    batch =
      BatchOperation.new_prepared(
        from,
        opcode,
        operation,
        item_count,
        item_router,
        payload_builder,
        opts
      )
      |> Map.put(:preparation_mode, :compact)
      |> Map.put(:group_preparer, group_preparer)

    if topology_version == TopologyManager.version(state.topology_manager),
      do: {:noreply, start_prepared(state, batch, groups, start_batch)},
      else: restart_stale_prepared_batch(state, batch, groups)
  end

  defp restart_stale_prepared_batch(state, batch, groups) do
    batch = %{
      batch
      | items: groups,
        item_restorer: KVBatchRestorer.new(batch.item_count, batch.operation),
        preparation_mode: :restore_compact
    }

    preparation_result(begin_preparation(state, batch))
  end

  @spec begin_preparation(State.t(), BatchOperation.t()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def begin_preparation(state, batch), do: BatchPreparationStarter.start(state, batch)

  defp start_prepared(state, batch, groups, start_batch) do
    timer = CoordinatorTimers.batch_timer(batch.id, batch.opts)
    caller_monitor = Process.monitor(elem(batch.from, 0))
    batch = %{batch | timer: timer, caller_monitor: caller_monitor}

    state
    |> State.put_lifecycle_monitor(caller_monitor, {:batch, batch.id})
    |> start_batch.(batch, groups)
  end

  defp preparation_result({:ok, state}), do: {:noreply, state}

  defp preparation_result({:error, reason, state}),
    do: {:reply, {:error, reason}, state}
end
