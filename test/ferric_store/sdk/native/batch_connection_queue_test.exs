defmodule FerricStore.SDK.Native.BatchConnectionQueueTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.BatchConnectionQueue

  test "waiting batches are unique and resume in FIFO order" do
    first = make_ref()
    second = make_ref()

    queue =
      %BatchConnectionQueue{}
      |> BatchConnectionQueue.enqueue(first, :first_endpoint)
      |> BatchConnectionQueue.enqueue(first, :first_endpoint)
      |> BatchConnectionQueue.enqueue(second, :second_endpoint)

    assert BatchConnectionQueue.size(queue) == 2
    assert {{:value, ^first}, queue} = BatchConnectionQueue.out(queue)
    assert {{:value, ^second}, queue} = BatchConnectionQueue.out(queue)
    assert {:empty, %BatchConnectionQueue{}} = BatchConnectionQueue.out(queue)
  end

  test "moving a batch does not reactivate stale queue entries" do
    moved = make_ref()
    peer = make_ref()

    globally_moved =
      %BatchConnectionQueue{}
      |> BatchConnectionQueue.enqueue(moved, :first_endpoint)
      |> BatchConnectionQueue.enqueue(peer, :second_endpoint)
      |> BatchConnectionQueue.enqueue(moved, :third_endpoint)

    assert {{:value, ^peer}, globally_moved} = BatchConnectionQueue.out(globally_moved)
    assert {{:value, ^moved}, globally_moved} = BatchConnectionQueue.out(globally_moved)
    assert {:empty, %BatchConnectionQueue{}} = BatchConnectionQueue.out(globally_moved)

    endpoint_moved =
      %BatchConnectionQueue{}
      |> BatchConnectionQueue.enqueue(moved, :ready_endpoint)
      |> BatchConnectionQueue.enqueue(peer, :ready_endpoint)
      |> BatchConnectionQueue.enqueue(moved, :other_endpoint)
      |> BatchConnectionQueue.enqueue(moved, :ready_endpoint)

    assert {[^peer, ^moved], endpoint_moved} =
             BatchConnectionQueue.take_endpoint(endpoint_moved, :ready_endpoint, 2)

    assert {:empty, %BatchConnectionQueue{}} = BatchConnectionQueue.out(endpoint_moved)
  end

  test "cancelled waiting batches do not accumulate scheduler tombstones" do
    survivor = make_ref()
    queue = BatchConnectionQueue.enqueue(%BatchConnectionQueue{}, survivor, :survivor_endpoint)

    queue =
      Enum.reduce(1..20_000, queue, fn _index, queue ->
        cancelled = make_ref()

        queue
        |> BatchConnectionQueue.enqueue(cancelled, :cancelled_endpoint)
        |> BatchConnectionQueue.delete(cancelled)
      end)

    assert BatchConnectionQueue.size(queue) == 1
    assert :queue.len(queue.order) < 100
    assert {{:value, ^survivor}, queue} = BatchConnectionQueue.out(queue)
    assert {:empty, %BatchConnectionQueue{}} = BatchConnectionQueue.out(queue)
  end

  test "an endpoint wakeup ignores unrelated waiting batches" do
    matching = Enum.map(1..10, fn _index -> make_ref() end)
    unrelated = Enum.map(1..10_000, fn _index -> make_ref() end)

    queue =
      Enum.reduce(unrelated, %BatchConnectionQueue{}, fn batch_id, queue ->
        BatchConnectionQueue.enqueue(queue, batch_id, :other_endpoint)
      end)

    queue =
      Enum.reduce(matching, queue, fn batch_id, queue ->
        BatchConnectionQueue.enqueue(queue, batch_id, :ready_endpoint)
      end)

    {:reductions, before} = Process.info(self(), :reductions)
    {resumed, queue} = BatchConnectionQueue.take_endpoint(queue, :ready_endpoint, 64)
    {:reductions, after_take} = Process.info(self(), :reductions)

    assert MapSet.new(resumed) == MapSet.new(matching)
    assert after_take - before < 5_000
    assert BatchConnectionQueue.endpoint_size(queue, :other_endpoint) == length(unrelated)
  end

  test "endpoint wakeups preserve FIFO order across bounded takes" do
    ready = Enum.map(1..128, fn _index -> make_ref() end)
    unrelated = make_ref()

    queue =
      Enum.reduce(ready, %BatchConnectionQueue{}, fn batch_id, queue ->
        BatchConnectionQueue.enqueue(queue, batch_id, :ready_endpoint)
      end)
      |> BatchConnectionQueue.enqueue(unrelated, :other_endpoint)

    assert {first_half, queue} = BatchConnectionQueue.take_endpoint(queue, :ready_endpoint, 64)
    assert first_half == Enum.take(ready, 64)

    assert {second_half, queue} = BatchConnectionQueue.take_endpoint(queue, :ready_endpoint, 64)
    assert second_half == Enum.drop(ready, 64)

    assert BatchConnectionQueue.endpoint_size(queue, :ready_endpoint) == 0
    assert {{:value, ^unrelated}, queue} = BatchConnectionQueue.out(queue)
    assert {:empty, %BatchConnectionQueue{}} = BatchConnectionQueue.out(queue)
  end

  test "randomized queue operations match a simple FIFO ownership model" do
    :rand.seed(:exsss, {91, 92, 93})
    batch_ids = Enum.map(1..64, fn _index -> make_ref() end)
    endpoints = [:a, :b, :c, :d]

    {_queue, _model} =
      Enum.reduce(1..20_000, {%BatchConnectionQueue{}, %{entries: %{}, order: []}}, fn step,
                                                                                       state ->
        state = random_operation(state, batch_ids, endpoints)
        if rem(step, 97) == 0, do: assert_model(state, endpoints), else: state
      end)
  end

  defp random_operation({queue, model}, batch_ids, endpoints) do
    batch_id = Enum.at(batch_ids, :rand.uniform(length(batch_ids)) - 1)
    endpoint = Enum.at(endpoints, :rand.uniform(length(endpoints)) - 1)

    case :rand.uniform(4) do
      1 ->
        {BatchConnectionQueue.enqueue(queue, batch_id, endpoint),
         model_enqueue(model, batch_id, endpoint)}

      2 ->
        {BatchConnectionQueue.delete(queue, batch_id), model_delete(model, batch_id)}

      3 ->
        assert_out(queue, model)

      4 ->
        assert_take_endpoint(queue, model, endpoint, :rand.uniform(8))
    end
  end

  defp assert_out(queue, %{order: []} = model) do
    assert {:empty, queue} = BatchConnectionQueue.out(queue)
    {queue, model}
  end

  defp assert_out(queue, %{order: [batch_id | rest], entries: entries} = model) do
    assert {{:value, ^batch_id}, queue} = BatchConnectionQueue.out(queue)
    {queue, %{model | order: rest, entries: Map.delete(entries, batch_id)}}
  end

  defp assert_take_endpoint(queue, model, endpoint, limit) do
    selected = model.order |> Enum.filter(&(model.entries[&1] == endpoint)) |> Enum.take(limit)
    assert {^selected, queue} = BatchConnectionQueue.take_endpoint(queue, endpoint, limit)

    selected_set = MapSet.new(selected)
    entries = Map.drop(model.entries, selected)
    order = Enum.reject(model.order, &MapSet.member?(selected_set, &1))
    {queue, %{model | entries: entries, order: order}}
  end

  defp assert_model({queue, model} = state, endpoints) do
    assert BatchConnectionQueue.size(queue) == map_size(model.entries)

    Enum.each(endpoints, fn endpoint ->
      assert BatchConnectionQueue.endpoint_size(queue, endpoint) ==
               Enum.count(model.entries, fn {_batch_id, value} -> value == endpoint end)
    end)

    state
  end

  defp model_enqueue(%{entries: entries} = model, batch_id, endpoint) do
    case Map.fetch(entries, batch_id) do
      {:ok, ^endpoint} ->
        model

      _missing_or_moved ->
        model
        |> model_delete(batch_id)
        |> then(fn model ->
          %{
            model
            | entries: Map.put(model.entries, batch_id, endpoint),
              order: model.order ++ [batch_id]
          }
        end)
    end
  end

  defp model_delete(%{entries: entries, order: order} = model, batch_id),
    do: %{model | entries: Map.delete(entries, batch_id), order: List.delete(order, batch_id)}
end
