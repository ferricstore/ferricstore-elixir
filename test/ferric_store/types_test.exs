defmodule FerricStore.TypesTest do
  use ExUnit.Case, async: false

  alias FerricStore.RequestContext
  alias FerricStore.Test.{ExplodingString, ThrowingInspect}
  alias FerricStore.Types

  test "get supports binary and existing atom keys without creating atoms" do
    assert Types.get(%{"status" => "binary", status: "atom"}, "status") == "binary"
    assert Types.get(%{status: "atom"}, "status") == "atom"

    prefix = "untrusted_lookup_#{System.unique_integer([:positive])}_"
    before_count = :erlang.system_info(:atom_count)

    Enum.each(1..1_000, fn index ->
      assert Types.get(%{}, prefix <> Integer.to_string(index), :missing) == :missing
    end)

    assert :erlang.system_info(:atom_count) == before_count
  end

  test "normalize_map rejects keys that collapse to the same string" do
    assert_raise ArgumentError, ~r/duplicate normalized map key.*status/, fn ->
      Types.normalize_map(%{:status => "atom", "status" => "binary"})
    end
  end

  test "normalize_map_keys normalizes only the routing-visible map level" do
    nested = %{status: :queued}

    assert %{"id" => "flow-id", "nested" => ^nested} =
             Types.normalize_map_keys(%{id: "flow-id", nested: nested})

    assert_raise ArgumentError, ~r/duplicate normalized map key.*id/, fn ->
      Types.normalize_map_keys(%{:id => "atom", "id" => "binary"})
    end
  end

  test "recursive map normalization reports improper nested lists without enumerable crashes" do
    improper = [%{id: "flow-id"} | :invalid_tail]

    assert {:error, :improper_list} = Types.normalize_map_result(%{items: improper})

    assert_raise ArgumentError, ~r/improper list/, fn ->
      Types.normalize_map(%{items: improper})
    end
  end

  test "recursive map normalization returns unsupported map keys as data" do
    invalid_key = {:unsupported, :map_key}
    value = %{"nested" => %{invalid_key => "value"}}

    assert {:error, {:invalid_map_key, ^invalid_key}} = Types.normalize_map_result(value)

    assert_raise ArgumentError, ~r/cannot normalize map key/, fn ->
      Types.normalize_map(value)
    end
  end

  test "map key conversion callbacks cannot throw through the public boundary" do
    key = %ExplodingString{}

    assert {:error, {:invalid_map_key, ^key}} = Types.normalize_map_result(%{key => "value"})
    assert {:error, {:invalid_map_key, ^key}} = Types.normalize_map_keys_result(%{key => "value"})
  end

  test "raising map helpers contain uninspectable invalid keys" do
    key = %ThrowingInspect{}

    assert_raise ArgumentError, "cannot normalize map key <unrenderable>", fn ->
      Types.normalize_map(%{key => "value"})
    end

    assert_raise ArgumentError, "cannot normalize map key <unrenderable>", fn ->
      Types.normalize_map_keys(%{key => "value"})
    end
  end

  test "recursive map normalization enforces native value depth and collection limits" do
    nested = Enum.reduce(1..65, "leaf", fn _level, value -> %{"nested" => value} end)

    assert {:error, :value_nesting_too_deep} = Types.normalize_map_result(nested)

    assert_raise ArgumentError, ~r/value nesting exceeds 64 levels/, fn ->
      Types.normalize_map(nested)
    end

    oversized = %{"items" => List.duplicate(nil, 100_001)}
    assert {:error, :collection_too_large} = Types.normalize_map_result(oversized)

    assert_raise ArgumentError, ~r/collection exceeds 100000 items/, fn ->
      Types.normalize_map(oversized)
    end
  end

  test "key-only map normalization rejects oversized maps before copying entries" do
    oversized = Map.new(1..100_001, &{&1, nil})
    :erlang.garbage_collect(self())
    {:reductions, before_count} = Process.info(self(), :reductions)

    assert {:error, :collection_too_large} = Types.normalize_map_keys_result(oversized)

    {:reductions, after_count} = Process.info(self(), :reductions)
    assert after_count - before_count < 10_000
  end

  test "budgeted map-key normalization uses countdown deadline checkpoints" do
    fields = Map.new(1..100_000, &{"field-#{&1}", &1})
    budget = RequestContext.new([timeout: :infinity], 5_000) |> RequestContext.budget()
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = Types.normalize_map_keys_result(fields, budget)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:ok, normalized} = result
    assert map_size(normalized) == 100_000
    assert after_reductions - before_reductions < 1_520_000
  end

  test "budgeted recursive normalization rejects an expired deadline before traversal" do
    value = %{"items" => Enum.to_list(1..100_000)}
    budget = RequestContext.new([timeout: 0], 5_000) |> RequestContext.budget()
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :timeout} = Types.normalize_map_result(value, budget)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 10_000
  end
end
