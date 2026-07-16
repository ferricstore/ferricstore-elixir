defmodule FerricStore.SDK.Native.RefreshCompletionQueue do
  @moduledoc false

  alias FerricStore.SDK.Native.RefreshOperation

  defstruct groups: {[], []}, keys: MapSet.new(), cancelled: MapSet.new()

  @type completion :: {RefreshOperation.waiter(), term()}
  @type group :: %{
          waiters: [RefreshOperation.waiter()],
          keys: MapSet.t(term()),
          cancelled: MapSet.t(term()),
          result: term()
        }
  @type t :: %__MODULE__{
          groups: :queue.queue(group()),
          keys: MapSet.t(term()),
          cancelled: MapSet.t(term())
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{groups: groups}), do: :queue.is_empty(groups)

  @spec enqueue(t(), RefreshOperation.t(), term()) :: t()
  def enqueue(%__MODULE__{} = queue, %RefreshOperation{} = operation, result) do
    group = %{
      waiters: Enum.reverse(operation.waiters),
      keys: operation.waiter_keys,
      cancelled: operation.cancelled_waiters,
      result: result
    }

    enqueue_group(queue, group)
  end

  @spec enqueue_waiters(t(), [RefreshOperation.waiter()], term()) :: t()
  def enqueue_waiters(%__MODULE__{} = queue, waiters, result) when is_list(waiters) do
    group = %{
      waiters: waiters,
      keys: waiters |> Enum.map(&RefreshOperation.waiter_key/1) |> MapSet.new(),
      cancelled: MapSet.new(),
      result: result
    }

    enqueue_group(queue, group)
  end

  @spec cancel(t(), term()) :: {:ok, t()} | :missing
  def cancel(%__MODULE__{} = queue, key) do
    if MapSet.member?(queue.keys, key) and not MapSet.member?(queue.cancelled, key) do
      {:ok,
       %{
         queue
         | cancelled: MapSet.put(queue.cancelled, key)
       }}
    else
      :missing
    end
  end

  @spec take(t(), non_neg_integer()) :: {[completion()], t()}
  def take(%__MODULE__{} = queue, limit) when is_integer(limit) and limit >= 0 do
    {completions, groups, keys, cancelled} =
      take_groups(queue.groups, limit, [], queue.keys, queue.cancelled)

    {Enum.reverse(completions), %{queue | groups: groups, keys: keys, cancelled: cancelled}}
  end

  @spec active_waiters(t()) :: [RefreshOperation.waiter()]
  def active_waiters(%__MODULE__{groups: groups, cancelled: cancelled}) do
    groups
    |> :queue.to_list()
    |> Enum.flat_map(fn group ->
      Enum.reject(group.waiters, fn waiter ->
        key = RefreshOperation.waiter_key(waiter)
        MapSet.member?(group.cancelled, key) or MapSet.member?(cancelled, key)
      end)
    end)
  end

  defp enqueue_group(%__MODULE__{} = queue, group) do
    group = deduplicate_active_waiters(group, queue.keys)
    do_enqueue_group(queue, group)
  end

  defp do_enqueue_group(queue, %{waiters: []}), do: queue

  defp do_enqueue_group(%__MODULE__{groups: groups} = queue, group) do
    %{
      queue
      | groups: :queue.in(group, groups),
        keys: MapSet.union(queue.keys, group.keys)
    }
  end

  defp deduplicate_active_waiters(group, queued_keys) do
    {waiters, keys, _seen} =
      Enum.reduce(group.waiters, {[], MapSet.new(), queued_keys}, fn waiter,
                                                                     {waiters, keys, seen} ->
        key = RefreshOperation.waiter_key(waiter)

        cond do
          MapSet.member?(group.cancelled, key) ->
            {[waiter | waiters], keys, seen}

          MapSet.member?(seen, key) ->
            {waiters, keys, seen}

          true ->
            {[waiter | waiters], MapSet.put(keys, key), MapSet.put(seen, key)}
        end
      end)

    %{group | waiters: Enum.reverse(waiters), keys: keys}
  end

  defp take_groups(groups, 0, completions, keys, cancelled),
    do: {completions, groups, keys, cancelled}

  defp take_groups(groups, limit, completions, keys, cancelled) do
    case :queue.out(groups) do
      {:empty, groups} ->
        {completions, groups, keys, cancelled}

      {{:value, group}, groups} ->
        {taken, group, remaining_limit, keys, cancelled} =
          take_group(group, limit, [], keys, cancelled)

        groups = if group.waiters == [], do: groups, else: :queue.in_r(group, groups)
        take_groups(groups, remaining_limit, taken ++ completions, keys, cancelled)
    end
  end

  defp take_group(group, 0, taken, keys, cancelled),
    do: {taken, group, 0, keys, cancelled}

  defp take_group(%{waiters: []} = group, limit, taken, keys, cancelled),
    do: {taken, group, limit, keys, cancelled}

  defp take_group(%{waiters: [waiter | rest]} = group, limit, taken, keys, cancelled) do
    key = RefreshOperation.waiter_key(waiter)
    active? = MapSet.member?(group.keys, key)

    cancelled? =
      MapSet.member?(group.cancelled, key) or MapSet.member?(cancelled, key)

    group = %{
      group
      | waiters: rest,
        keys: MapSet.delete(group.keys, key),
        cancelled: MapSet.delete(group.cancelled, key)
    }

    keys = if active?, do: MapSet.delete(keys, key), else: keys
    cancelled = if active?, do: MapSet.delete(cancelled, key), else: cancelled

    if cancelled? do
      take_group(group, limit - 1, taken, keys, cancelled)
    else
      take_group(group, limit - 1, [{waiter, group.result} | taken], keys, cancelled)
    end
  end
end
