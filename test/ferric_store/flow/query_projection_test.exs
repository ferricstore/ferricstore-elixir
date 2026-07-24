defmodule FerricStore.Flow.QueryProjectionTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.QueryProjection

  test "builds source-aware run and event projections with escaped names" do
    assert {:ok, query} =
             QueryProjection.project(
               "FROM runs WHERE run_id = @id",
               :record,
               [
                 :run_id,
                 :state,
                 {:attribute, "customer.tier"},
                 {:state_meta, "review's", "risk tier"}
               ]
             )

    assert query ==
             "FROM runs WHERE run_id = @id RETURN RECORD " <>
               "(run_id, state, attribute['customer.tier'], " <>
               "state_meta['review''s']['risk tier'])"

    assert {:ok, event_query} =
             QueryProjection.project(
               " FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20; ",
               :records,
               [:event_id, {:event_field, "worker's.pool"}]
             )

    assert event_query ==
             "FROM events WHERE run_id = @id ORDER BY event_id ASC LIMIT 20 " <>
               "RETURN RECORDS (event_id, fields['worker''s.pool'])"
  end

  test "rejects wrong sources, duplicates, invalid fields, and existing returns" do
    assert {:error, {:invalid_flow_query_projection, :field_source}} =
             QueryProjection.project(
               "FROM runs WHERE run_id = @id",
               :record,
               [:event_id]
             )

    assert {:error, {:invalid_flow_query_projection, :duplicate_field}} =
             QueryProjection.project(
               "FROM runs WHERE run_id = @id",
               :record,
               [:state, :state]
             )

    assert {:error, {:invalid_flow_query_projection, :return_clause_present}} =
             QueryProjection.project(
               "FROM runs WHERE type = 'RETURN' RETURN RECORD",
               :record,
               [:run_id]
             )
  end

  test "bounds field counts, dynamic names, and final query bytes" do
    assert {:error, {:invalid_flow_query_projection, :field_limit}} =
             QueryProjection.project("FROM runs WHERE run_id = @id", :records, [])

    assert {:error, {:invalid_flow_query_projection, :field_limit}} =
             QueryProjection.project(
               "FROM runs WHERE run_id = @id",
               :records,
               for(index <- 1..33, do: {:attribute, "field_#{index}"})
             )

    assert {:error, {:invalid_flow_query_projection, :invalid_dynamic_name}} =
             QueryProjection.project(
               "FROM events WHERE run_id = @id",
               :record,
               [{:event_field, String.duplicate("x", 65)}]
             )

    assert {:error, {:invalid_flow_query_projection, :invalid_query}} =
             QueryProjection.project(
               "FROM runs WHERE type = '" <> String.duplicate("x", 16_350) <> "'",
               :record,
               [:run_id]
             )
  end
end
