defmodule FerricStore.SDK.Native.LifecycleRegistry do
  @moduledoc false

  defstruct owners: %{}

  @type owner ::
          {:pending_request, reference()}
          | {:preparation_reservation, reference()}
          | {:refresh_waiter, reference()}
          | {:batch, reference()}
          | {:batch_preparer, reference()}
          | {:event_call, reference()}
          | {:connection, pid()}
          | {:connection_attempt, term()}
          | {:topology_refresh, reference()}
          | {:event_subscriber, pid()}

  @type t :: %__MODULE__{owners: %{optional(reference()) => owner()}}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{owners: owners}), do: map_size(owners) == 0

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{owners: owners}), do: map_size(owners)

  @spec get(t(), reference()) :: owner() | nil
  def get(%__MODULE__{owners: owners}, monitor) when is_reference(monitor),
    do: Map.get(owners, monitor)

  @spec put(t(), reference(), owner()) :: t()
  def put(%__MODULE__{} = registry, monitor, {:pending_request, tag})
      when is_reference(monitor) and is_reference(tag),
      do: put_owner(registry, monitor, {:pending_request, tag})

  def put(%__MODULE__{} = registry, monitor, {:preparation_reservation, token})
      when is_reference(monitor) and is_reference(token),
      do: put_owner(registry, monitor, {:preparation_reservation, token})

  def put(%__MODULE__{} = registry, monitor, {:refresh_waiter, waiter_monitor})
      when is_reference(monitor) and is_reference(waiter_monitor),
      do: put_owner(registry, monitor, {:refresh_waiter, waiter_monitor})

  def put(%__MODULE__{} = registry, monitor, {:batch, batch_id})
      when is_reference(monitor) and is_reference(batch_id),
      do: put_owner(registry, monitor, {:batch, batch_id})

  def put(%__MODULE__{} = registry, monitor, {:batch_preparer, batch_id})
      when is_reference(monitor) and is_reference(batch_id),
      do: put_owner(registry, monitor, {:batch_preparer, batch_id})

  def put(%__MODULE__{} = registry, monitor, {:event_call, event_call_id})
      when is_reference(monitor) and is_reference(event_call_id),
      do: put_owner(registry, monitor, {:event_call, event_call_id})

  def put(%__MODULE__{} = registry, monitor, {:connection, connection})
      when is_reference(monitor) and is_pid(connection),
      do: put_owner(registry, monitor, {:connection, connection})

  def put(%__MODULE__{} = registry, monitor, {:connection_attempt, key})
      when is_reference(monitor),
      do: put_owner(registry, monitor, {:connection_attempt, key})

  def put(%__MODULE__{} = registry, monitor, {:topology_refresh, token})
      when is_reference(monitor) and is_reference(token),
      do: put_owner(registry, monitor, {:topology_refresh, token})

  def put(%__MODULE__{} = registry, monitor, {:event_subscriber, subscriber})
      when is_reference(monitor) and is_pid(subscriber),
      do: put_owner(registry, monitor, {:event_subscriber, subscriber})

  @spec delete(t(), reference(), owner()) :: t()
  def delete(%__MODULE__{} = registry, monitor, owner) when is_reference(monitor) do
    if Map.get(registry.owners, monitor) == owner do
      %{registry | owners: Map.delete(registry.owners, monitor)}
    else
      registry
    end
  end

  @spec pop(t(), reference()) :: {owner() | nil, t()}
  def pop(%__MODULE__{} = registry, monitor) when is_reference(monitor) do
    {owner, owners} = Map.pop(registry.owners, monitor)
    {owner, %{registry | owners: owners}}
  end

  defp put_owner(registry, monitor, owner),
    do: %{registry | owners: Map.put(registry.owners, monitor, owner)}
end
