defmodule FerricStore.Flow.BatchItemAdmissionTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow

  test "create-many rejects over-wide item maps before traversing their fields" do
    item = oversized_item(%{"id" => "flow"})

    assert_fast_rejection(fn -> Flow.create_many_payload([item], type: "email") end,
      invalid: :invalid_flow_create_many_item
    )
  end

  test "complete-many rejects over-wide item maps before traversing their fields" do
    item =
      oversized_item(%{
        "id" => "flow",
        "lease_token" => "lease",
        "fencing_token" => 1
      })

    assert_fast_rejection(fn -> Flow.complete_many_payload([item]) end,
      invalid: :invalid_flow_complete_many_item
    )
  end

  defp oversized_item(required) do
    fields = Map.new(1..25_000, &{"unexpected-#{&1}", &1})
    Map.merge(fields, required)
  end

  defp assert_fast_rejection(fun, invalid: reason) do
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    result = fun.()
    {:reductions, after_reductions} = Process.info(self(), :reductions)

    assert match?({:error, {^reason, _item}}, result)
    assert after_reductions - before_reductions < 50_000
  end
end
