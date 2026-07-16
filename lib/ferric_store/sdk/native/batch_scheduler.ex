defmodule FerricStore.SDK.Native.BatchScheduler do
  @moduledoc false

  alias FerricStore.SDK.Native.{BatchConnectionQueue, BatchOperation}

  defstruct batches: %{},
            waiting_connections: %BatchConnectionQueue{},
            waiting_wire_slots: %BatchConnectionQueue{}

  @type batch :: BatchOperation.t()
  @type t :: %__MODULE__{
          batches: %{reference() => batch()},
          waiting_connections: BatchConnectionQueue.t(),
          waiting_wire_slots: BatchConnectionQueue.t()
        }

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{batches: batches}), do: map_size(batches)

  @spec batches(t()) :: %{reference() => batch()}
  def batches(%__MODULE__{batches: batches}), do: batches

  @spec values(t()) :: [batch()]
  def values(%__MODULE__{batches: batches}), do: Map.values(batches)

  @spec get(t(), reference()) :: batch() | nil
  def get(%__MODULE__{batches: batches}, batch_id), do: Map.get(batches, batch_id)

  @spec fetch!(t(), reference()) :: batch()
  def fetch!(%__MODULE__{batches: batches}, batch_id), do: Map.fetch!(batches, batch_id)

  @spec put(t(), batch()) :: t()
  def put(%__MODULE__{} = scheduler, %{id: batch_id} = batch) do
    %{scheduler | batches: Map.put(scheduler.batches, batch_id, batch)}
  end

  @spec pop(t(), reference()) :: {batch() | nil, t()}
  def pop(%__MODULE__{} = scheduler, batch_id) do
    {batch, batches} = Map.pop(scheduler.batches, batch_id)

    scheduler = %{
      scheduler
      | batches: batches,
        waiting_connections: BatchConnectionQueue.delete(scheduler.waiting_connections, batch_id),
        waiting_wire_slots: BatchConnectionQueue.delete(scheduler.waiting_wire_slots, batch_id)
    }

    {batch, scheduler}
  end

  @spec waiting_size(t()) :: non_neg_integer()
  def waiting_size(%__MODULE__{waiting_connections: queue}),
    do: BatchConnectionQueue.size(queue)

  @spec wait_for_connection(t(), reference(), term()) :: t()
  def wait_for_connection(%__MODULE__{} = scheduler, batch_id, endpoint_key) do
    queue =
      BatchConnectionQueue.enqueue(scheduler.waiting_connections, batch_id, endpoint_key)

    %{scheduler | waiting_connections: queue}
  end

  @spec clear_connection_wait(t(), reference()) :: t()
  def clear_connection_wait(%__MODULE__{} = scheduler, batch_id) do
    queue = BatchConnectionQueue.delete(scheduler.waiting_connections, batch_id)
    %{scheduler | waiting_connections: queue}
  end

  @spec pop_connection_waiter(t()) :: {{:value, reference()}, t()} | {:empty, t()}
  def pop_connection_waiter(%__MODULE__{} = scheduler) do
    case BatchConnectionQueue.out(scheduler.waiting_connections) do
      {{:value, batch_id}, queue} ->
        {{:value, batch_id}, %{scheduler | waiting_connections: queue}}

      {:empty, queue} ->
        {:empty, %{scheduler | waiting_connections: queue}}
    end
  end

  @spec take_endpoint_waiters(t(), term(), pos_integer()) :: {[reference()], t()}
  def take_endpoint_waiters(%__MODULE__{} = scheduler, endpoint_key, limit) do
    {batch_ids, queue} =
      BatchConnectionQueue.take_endpoint(scheduler.waiting_connections, endpoint_key, limit)

    {batch_ids, %{scheduler | waiting_connections: queue}}
  end

  @spec endpoint_waiting_size(t(), term()) :: non_neg_integer()
  def endpoint_waiting_size(%__MODULE__{waiting_connections: queue}, endpoint_key),
    do: BatchConnectionQueue.endpoint_size(queue, endpoint_key)

  @spec wire_waiting_size(t()) :: non_neg_integer()
  def wire_waiting_size(%__MODULE__{waiting_wire_slots: queue}),
    do: BatchConnectionQueue.size(queue)

  @spec wait_for_wire_slot(t(), reference()) :: t()
  def wait_for_wire_slot(%__MODULE__{} = scheduler, batch_id) do
    queue = BatchConnectionQueue.enqueue(scheduler.waiting_wire_slots, batch_id, :global)
    %{scheduler | waiting_wire_slots: queue}
  end

  @spec clear_wire_wait(t(), reference()) :: t()
  def clear_wire_wait(%__MODULE__{} = scheduler, batch_id) do
    queue = BatchConnectionQueue.delete(scheduler.waiting_wire_slots, batch_id)
    %{scheduler | waiting_wire_slots: queue}
  end

  @spec take_wire_waiters(t(), pos_integer()) :: {[reference()], t()}
  def take_wire_waiters(%__MODULE__{} = scheduler, limit) do
    {batch_ids, queue} =
      BatchConnectionQueue.take_endpoint(scheduler.waiting_wire_slots, :global, limit)

    {batch_ids, %{scheduler | waiting_wire_slots: queue}}
  end
end
