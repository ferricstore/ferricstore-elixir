defmodule FerricStore.SDK.Native.KVPreparationCoordinator do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    BatchCoordinator,
    BatchOperation,
    CoordinatorTimers,
    KVPreparedRequest,
    PreparationReservations
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec admit(State.t(), GenServer.from(), non_neg_integer(), RequestContext.t()) ::
          {:reply, term(), State.t()}
  def admit(state, from, item_count, %RequestContext{} = context)
      when is_integer(item_count) and item_count >= 0 do
    if CoordinatorTimers.expired?(context),
      do: {:reply, {:error, :timeout}, state},
      else: reserve_if_admitted(state, elem(from, 0), item_count, context)
  end

  @spec dispatch_prepared(
          State.t(),
          GenServer.from(),
          KVPreparedRequest.t(),
          (State.t(), BatchOperation.t(), [map()] -> State.t())
        ) :: {:reply, term(), State.t()} | {:noreply, State.t()}
  def dispatch_prepared(
        state,
        from,
        %KVPreparedRequest{
          reservation: reservation,
          item_count: item_count,
          opts: %RequestContext{} = context
        } = prepared,
        start_batch
      )
      when is_function(start_batch, 3) do
    dispatch_consumed(
      consume(state, reservation, elem(from, 0), item_count),
      from,
      prepared,
      context,
      start_batch
    )
  end

  @spec release(State.t(), pid(), reference()) :: State.t()
  def release(state, owner, reservation) when is_pid(owner) and is_reference(reservation) do
    {_result, state} = release_owned(state, reservation, owner)
    state
  end

  @spec drop(State.t(), reference()) :: State.t()
  def drop(state, reservation) when is_reference(reservation) do
    case PreparationReservations.take(state.preparation_reservations, reservation) do
      {nil, _preparation_reservations} ->
        state

      {entry, preparation_reservations} ->
        state
        |> Map.put(:preparation_reservations, preparation_reservations)
        |> State.delete_lifecycle_monitor(
          entry.monitor,
          {:preparation_reservation, reservation}
        )
    end
  end

  defp reserve_if_admitted(state, owner, item_count, context) do
    case BatchCoordinator.admit(state, item_count) do
      :ok ->
        {reservation, state} = reserve(state, owner, item_count, context)
        {:reply, {:ok, reservation}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp reserve(state, owner, item_count, context) do
    {reservation, entry, preparation_reservations} =
      PreparationReservations.reserve(
        state.preparation_reservations,
        owner,
        item_count,
        context
      )

    state = %{state | preparation_reservations: preparation_reservations}

    state =
      State.put_lifecycle_monitor(
        state,
        entry.monitor,
        {:preparation_reservation, reservation}
      )

    {reservation, state}
  end

  defp consume(state, nil, _owner, _item_count), do: {:ok, state, :unreserved}

  defp consume(state, reservation, owner, item_count) do
    case PreparationReservations.fetch(state.preparation_reservations, reservation) do
      {:ok, %{owner: ^owner, item_count: ^item_count}} ->
        {:ok, drop(state, reservation), :reserved}

      {:ok, %{owner: ^owner}} ->
        {:error, :preparation_item_count_mismatch, state}

      {:ok, _other_owner} ->
        {:error, :invalid_preparation_owner, state}

      :error ->
        {:error, :invalid_preparation_reservation, state}
    end
  end

  defp dispatch_consumed({:ok, state, mode}, from, prepared, context, start_batch) do
    if CoordinatorTimers.expired?(context) do
      {:reply, {:error, :timeout}, state}
    else
      dispatch_for_mode(mode, state, from, prepared, start_batch)
    end
  end

  defp dispatch_consumed({:error, reason, state}, _from, _prepared, context, _start_batch) do
    reason = if CoordinatorTimers.expired?(context), do: :timeout, else: reason
    {:reply, {:error, reason}, state}
  end

  defp dispatch_for_mode(:reserved, state, from, prepared, start_batch),
    do: BatchCoordinator.dispatch_reserved_prepared_items(state, from, prepared, start_batch)

  defp dispatch_for_mode(:unreserved, state, from, prepared, start_batch),
    do: BatchCoordinator.dispatch_prepared_items(state, from, prepared, start_batch)

  defp release_owned(state, reservation, owner) do
    case PreparationReservations.fetch(state.preparation_reservations, reservation) do
      {:ok, %{owner: ^owner}} -> {:ok, drop(state, reservation)}
      {:ok, _other_owner} -> {{:error, :invalid_preparation_owner}, state}
      :error -> {:ok, state}
    end
  end
end
