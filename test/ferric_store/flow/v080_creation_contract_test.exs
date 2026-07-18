defmodule FerricStore.Flow.V080CreationContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Codec.Raw
  alias FerricStore.Flow
  alias FerricStore.Flow.ResponseRecords

  test "create-many carries type defaults and per-item max_active_ms values" do
    assert %{
             "type" => "email",
             "max_active_ms" => :infinity,
             "items" => [
               %{"id" => "finite", "payload" => "body", "max_active_ms" => 60_000},
               %{"id" => "unbounded", "max_active_ms" => "infinity"}
             ]
           } =
             Flow.create_many_payload(
               [
                 %{id: "finite", payload: "body", max_active_ms: 60_000},
                 %{"id" => "unbounded", "max_active_ms" => "infinity"}
               ],
               type: "email",
               max_active_ms: :infinity
             )
  end

  test "create-many rejects invalid per-item max_active_ms locally" do
    invalid = %{id: "invalid", max_active_ms: 0}

    assert {:error, {:invalid_flow_create_many_item, ^invalid}} =
             Flow.create_many_payload([invalid], type: "email")
  end

  test "create-many rejects invalid type-level max_active_ms before transport" do
    assert {:error,
            %FerricStore.Error{
              raw:
                {:invalid_flow_option, :create_many, :max_active_ms,
                 :expected_positive_bounded_duration_or_infinity}
            }} = Flow.create_many(self(), ["flow-1"], type: "email", max_active_ms: 0)
  end

  test "type policies accept the canonical infinity value" do
    assert %{"type" => "email", "max_active_ms" => :infinity} =
             Flow.policy_set_payload("email", max_active_ms: :infinity)
  end

  test "max_active_ms terminal failures preserve their structured reason" do
    record = %{"id" => "flow-1", "state" => "failed", "error" => %{"reason" => "max_active_ms"}}

    assert ResponseRecords.decode_record(record, Raw) == record
  end
end
