defmodule FerricStore.Architecture.FlowQueryLimitsTest do
  use ExUnit.Case, async: true

  alias FerricStore.FlowQueryLimits

  test "one contract owns shared FQL request, projection, and result limits" do
    assert FlowQueryLimits.max_query_bytes() == 16 * 1_024
    assert FlowQueryLimits.max_records() == 100
    assert FlowQueryLimits.max_projection_fields() == 32

    limits_source =
      "../../../lib/ferric_store/flow_query_limits.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    assert length(String.split(limits_source, "\n")) <= 20

    for path <- [
          "../../../lib/ferric_store/flow/query_builder_core.ex",
          "../../../lib/ferric_store/flow/query_response/result.ex",
          "../../../lib/ferric_store/flow/record_response_decoder.ex",
          "../../../lib/ferric_store/protocol/flow_query_record_decoder.ex"
        ] do
      source = path |> Path.expand(__DIR__) |> File.read!()
      assert source =~ "FlowQueryLimits.max_records()"
      refute source =~ ~r/@max_(records|results) 100/
    end

    for path <- [
          "../../../lib/ferric_store/flow/query_request.ex",
          "../../../lib/ferric_store/flow/query_projection.ex"
        ] do
      source = path |> Path.expand(__DIR__) |> File.read!()
      assert source =~ "FlowQueryLimits.max_query_bytes()"
      refute source =~ "@max_query_bytes 16 * 1_024"
    end

    projection =
      "../../../lib/ferric_store/flow/query_projection.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    assert projection =~ "FlowQueryLimits.max_projection_fields()"
    refute projection =~ "@max_fields 32"
  end
end
