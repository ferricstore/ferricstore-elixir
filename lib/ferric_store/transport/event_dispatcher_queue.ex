defmodule FerricStore.Transport.EventDispatcherQueue do
  @moduledoc false

  @compact_min_tombstones 64

  @spec initialize(map(), pos_integer()) :: map()
  def initialize(state, max_queue) when is_integer(max_queue) and max_queue > 0 do
    Map.merge(state, %{
      queue: :queue.new(),
      admissions: %{},
      queue_tombstones: 0,
      max_queue: max_queue
    })
  end

  @spec prepare(map(), reference(), term()) ::
          {map(), :ok | :dropped | :dropped_oldest, reference() | nil}
  def prepare(state, request_ref, event) when is_reference(request_ref) do
    capacity = state.max_queue + if(is_nil(state.busy), do: 1, else: 0)

    cond do
      Map.has_key?(state.admissions, request_ref) ->
        {record_drop(state), :dropped, nil}

      map_size(state.admissions) < capacity ->
        {enqueue(state, request_ref, event), :ok, nil}

      true ->
        {state, evicted_request_ref} = drop_oldest(state)
        state = record_drop(state)
        {enqueue(state, request_ref, event), :dropped_oldest, evicted_request_ref}
    end
  end

  @spec commit(map(), reference()) :: map()
  def commit(state, request_ref) when is_reference(request_ref) do
    case Map.fetch(state.admissions, request_ref) do
      {:ok, admission} ->
        admission = %{admission | committed?: true}
        %{state | admissions: Map.put(state.admissions, request_ref, admission)}

      :error ->
        state
    end
  end

  @spec cancel(map(), reference()) :: map()
  def cancel(state, request_ref) when is_reference(request_ref) do
    case Map.pop(state.admissions, request_ref) do
      {nil, _admissions} ->
        state

      {_admission, admissions} ->
        state
        |> Map.put(:admissions, admissions)
        |> Map.update!(:queue_tombstones, &(&1 + 1))
        |> compact()
    end
  end

  @spec take_committed(map()) :: {:ok, term(), map()} | {:blocked | :empty, map()}
  def take_committed(state) do
    case :queue.out(state.queue) do
      {{:value, request_ref}, queue} ->
        take_queued(state, queue, request_ref)

      {:empty, _queue} ->
        {:empty, reset_empty(state)}
    end
  end

  @spec drop_uncommitted(map()) :: map()
  def drop_uncommitted(state) do
    {admissions, dropped} =
      Enum.reduce(state.admissions, {%{}, 0}, fn
        {request_ref, %{committed?: true} = admission}, {admissions, dropped} ->
          {Map.put(admissions, request_ref, admission), dropped}

        {_request_ref, %{committed?: false}}, {admissions, dropped} ->
          {admissions, dropped + 1}
      end)

    queue = rebuild_queue(state.queue, admissions)

    %{
      state
      | admissions: admissions,
        queue: queue,
        queue_tombstones: 0,
        dropped: state.dropped + dropped
    }
  end

  @spec empty?(map()) :: boolean()
  def empty?(state), do: map_size(state.admissions) == 0

  @spec size(map()) :: non_neg_integer()
  def size(state), do: map_size(state.admissions)

  defp enqueue(state, request_ref, event) do
    admission = %{event: event, committed?: false}

    %{
      state
      | queue: :queue.in(request_ref, state.queue),
        admissions: Map.put(state.admissions, request_ref, admission)
    }
  end

  defp record_drop(state), do: Map.update!(state, :dropped, &(&1 + 1))

  defp drop_oldest(state) do
    case :queue.out(state.queue) do
      {{:value, request_ref}, queue} ->
        case Map.pop(state.admissions, request_ref) do
          {nil, _admissions} ->
            drop_oldest(%{
              state
              | queue: queue,
                queue_tombstones: state.queue_tombstones - 1
            })

          {_admission, admissions} ->
            {%{state | queue: queue, admissions: admissions}, request_ref}
        end

      {:empty, _queue} ->
        {reset_empty(state), nil}
    end
  end

  defp take_queued(state, queue, request_ref) do
    case Map.fetch(state.admissions, request_ref) do
      :error ->
        state
        |> Map.put(:queue, queue)
        |> Map.update!(:queue_tombstones, &(&1 - 1))
        |> take_committed()

      {:ok, %{committed?: false}} ->
        {:blocked, state}

      {:ok, %{committed?: true, event: event}} ->
        state = %{
          state
          | queue: queue,
            admissions: Map.delete(state.admissions, request_ref)
        }

        {:ok, event, state}
    end
  end

  defp compact(state) do
    cond do
      empty?(state) ->
        reset_empty(state)

      state.queue_tombstones >= @compact_min_tombstones and
          state.queue_tombstones > size(state) ->
        %{state | queue: rebuild_queue(state.queue, state.admissions), queue_tombstones: 0}

      true ->
        state
    end
  end

  defp rebuild_queue(queue, admissions) do
    queue
    |> :queue.to_list()
    |> Enum.filter(&Map.has_key?(admissions, &1))
    |> :queue.from_list()
  end

  defp reset_empty(state),
    do: %{state | queue: :queue.new(), queue_tombstones: 0}
end
