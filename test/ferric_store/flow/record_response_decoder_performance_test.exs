defmodule FerricStore.Flow.RecordResponseDecoderPerformanceTest do
  use ExUnit.Case, async: true

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.RecordResponseDecoder

  test "raw record validation returns the original list" do
    records = Enum.map(1..100, &%{"id" => "run-#{&1}", "state" => "queued"})

    decoded =
      RecordResponseDecoder.decode_list_raw(
        records,
        :list,
        DeadlineBudget.new(1_000)
      )

    assert :erts_debug.same(records, decoded)
  end

  test "raw validation retains malformed-response error precedence" do
    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_response, %{operation: :list, reason: :expected_record_map}}
            }} =
             RecordResponseDecoder.decode_list_raw(
               [%{"id" => "valid"}, :invalid],
               :list,
               DeadlineBudget.new(1_000)
             )

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_response, %{operation: :list, reason: :expected_record_list}}
            }} =
             RecordResponseDecoder.decode_list_raw(
               [%{"id" => "valid"} | :improper],
               :list,
               DeadlineBudget.new(1_000)
             )
  end

  test "typed query response validation uses one bounded record traversal" do
    source =
      "../../../lib/ferric_store/flow/query_response/result.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    assert source =~ "BoundedListValidator.validate(records"
    refute source =~ "length(records)"
    refute source =~ "Enum.all?(records"
  end
end
