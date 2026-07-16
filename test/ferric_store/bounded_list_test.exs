defmodule FerricStore.BoundedListTest do
  use ExUnit.Case, async: true

  alias FerricStore.{BoundedList, DeadlineBudget}

  test "map admits the complete list before invoking the mapper" do
    counter = :atomics.new(1, [])

    mapper = fn item ->
      :atomics.add(counter, 1, 1)
      item
    end

    assert {:error, {:limit_exceeded, 3}} =
             BoundedList.map([:one, :two, :three], 2, mapper)

    assert :atomics.get(counter, 1) == 0
  end

  test "improper lists are rejected before mapping" do
    counter = :atomics.new(1, [])

    mapper = fn item ->
      :atomics.add(counter, 1, 1)
      item
    end

    assert {:error, :improper_list} = BoundedList.count([:one | :tail], 10)
    assert {:error, :improper_list} = BoundedList.map([:one | :tail], 10, mapper)
    assert :atomics.get(counter, 1) == 0
  end

  test "map with count returns the admitted size without recounting the mapped list" do
    assert {:ok, 3, [2, 4, 6]} = BoundedList.map_with_count([1, 2, 3], 3, &(&1 * 2))
  end

  test "result mapping admits the complete list before mapping and preserves mapper errors" do
    counter = :atomics.new(1, [])

    mapper = fn item ->
      :atomics.add(counter, 1, 1)
      if item == :invalid, do: {:error, :invalid_item}, else: {:ok, item}
    end

    assert {:error, {:limit_exceeded, 3}} =
             BoundedList.map_result_with_count([:one, :two, :invalid], 2, mapper)

    assert :atomics.get(counter, 1) == 0

    assert {:error, :invalid_item} =
             BoundedList.map_result_with_count([:one, :invalid], 2, mapper)
  end

  test "budgeted counting stops before traversing an expired input" do
    items = List.duplicate(:item, 100_000)
    budget = DeadlineBudget.new(0)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :timeout} = BoundedList.count(items, 100_000, budget)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
  end
end
