defmodule FerricStore.SDK.Native.TopologyManager do
  @moduledoc false

  alias FerricStore.SDK.Native.{RefreshCompletionQueue, RefreshOperation, Topology}

  defstruct topology: nil,
            version: nil,
            refresh_operation: nil,
            refresh_completions: %RefreshCompletionQueue{},
            refresh_calls: 0

  @type t :: %__MODULE__{
          topology: Topology.t() | nil,
          version: reference() | nil,
          refresh_operation: RefreshOperation.t() | nil,
          refresh_completions: RefreshCompletionQueue.t(),
          refresh_calls: non_neg_integer()
        }

  @spec topology(t()) :: Topology.t() | nil
  def topology(%__MODULE__{topology: topology}), do: topology

  @spec put_topology(t(), Topology.t()) :: t()
  def put_topology(%__MODULE__{topology: topology} = manager, %Topology{} = topology),
    do: manager

  def put_topology(%__MODULE__{} = manager, %Topology{} = topology),
    do: %{manager | topology: topology, version: make_ref()}

  @spec snapshot(t()) :: {reference() | nil, Topology.t() | nil}
  def snapshot(%__MODULE__{version: version, topology: topology}), do: {version, topology}

  @spec version(t()) :: reference() | nil
  def version(%__MODULE__{version: version}), do: version

  @spec route(t(), binary()) :: {:ok, map()} | {:error, term()}
  def route(%__MODULE__{topology: topology}, key), do: Topology.route_key(topology, key)

  @spec refresh_operation(t()) :: map() | nil
  def refresh_operation(%__MODULE__{refresh_operation: operation}), do: operation

  @spec put_refresh_operation(t(), map() | nil) :: t()
  def put_refresh_operation(%__MODULE__{} = manager, operation),
    do: %{manager | refresh_operation: operation}

  @spec enqueue_refresh_completion(t(), RefreshOperation.t(), term()) :: t()
  def enqueue_refresh_completion(%__MODULE__{} = manager, %RefreshOperation{} = operation, result) do
    completions = RefreshCompletionQueue.enqueue(manager.refresh_completions, operation, result)
    %{manager | refresh_completions: completions}
  end

  @spec enqueue_refresh_waiters(t(), [RefreshOperation.waiter()], term()) :: t()
  def enqueue_refresh_waiters(%__MODULE__{} = manager, waiters, result) do
    completions =
      RefreshCompletionQueue.enqueue_waiters(manager.refresh_completions, waiters, result)

    %{manager | refresh_completions: completions}
  end

  @spec take_refresh_completions(t(), non_neg_integer()) ::
          {[RefreshCompletionQueue.completion()], t()}
  def take_refresh_completions(%__MODULE__{} = manager, limit) do
    {items, completions} = RefreshCompletionQueue.take(manager.refresh_completions, limit)
    {items, %{manager | refresh_completions: completions}}
  end

  @spec cancel_refresh_completion(t(), term()) :: {:ok, t()} | :missing
  def cancel_refresh_completion(%__MODULE__{} = manager, key) do
    case RefreshCompletionQueue.cancel(manager.refresh_completions, key) do
      {:ok, completions} -> {:ok, %{manager | refresh_completions: completions}}
      :missing -> :missing
    end
  end

  @spec refresh_completions_empty?(t()) :: boolean()
  def refresh_completions_empty?(%__MODULE__{} = manager),
    do: RefreshCompletionQueue.empty?(manager.refresh_completions)

  @spec refresh_completion_waiters(t()) :: [RefreshOperation.waiter()]
  def refresh_completion_waiters(%__MODULE__{} = manager),
    do: RefreshCompletionQueue.active_waiters(manager.refresh_completions)

  @spec refresh_call_count(t()) :: non_neg_integer()
  def refresh_call_count(%__MODULE__{refresh_calls: count}), do: count

  @spec adjust_refresh_calls(t(), integer()) :: t()
  def adjust_refresh_calls(%__MODULE__{} = manager, delta) when is_integer(delta),
    do: %{manager | refresh_calls: max(manager.refresh_calls + delta, 0)}
end
