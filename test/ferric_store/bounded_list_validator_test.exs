defmodule FerricStore.BoundedListValidatorTest do
  use ExUnit.Case, async: true

  alias FerricStore.{BoundedListValidator, DeadlineBudget}

  test "validates and counts a bounded proper list without constructing a replacement" do
    assert {:ok, 3} = BoundedListValidator.validate([1, 2, 3], 3, &is_integer/1)
    assert {:error, :invalid_item} = BoundedListValidator.validate([1, :two], 3, &is_integer/1)
    assert {:error, :expected_list} = BoundedListValidator.validate([1 | :tail], 3, &is_integer/1)
  end

  test "stops before inspecting an item beyond the bound" do
    Process.put(:validated_items, 0)

    predicate = fn _item ->
      Process.put(:validated_items, Process.get(:validated_items) + 1)
      true
    end

    assert {:error, :too_large} =
             BoundedListValidator.validate(Enum.to_list(1..101), 100, predicate)

    assert Process.get(:validated_items) == 100
  end

  test "honors an expired deadline before traversing" do
    assert {:error, :timeout} =
             BoundedListValidator.validate(
               Enum.to_list(1..100),
               100,
               &is_integer/1,
               DeadlineBudget.new(0)
             )
  end
end
