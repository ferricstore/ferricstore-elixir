defmodule FerricStore.Flow.ConvenienceProjectionTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    def start_link(owner) do
      GenServer.start_link(__MODULE__, owner)
      |> ClientRuntime.wrap()
    end

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, owner) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, owner)
    end

    def handle_call({:request, _opcode, payload, _context}, _from, owner) do
      send(owner, {:flow_query_payload, payload})
      {:reply, {:ok, query_response()}, owner}
    end

    defp query_response do
      %{
        "version" => "ferric.flow.query.result/v1",
        "records" => [],
        "page" => %{"has_more" => false, "cursor" => nil},
        "quality" => %{
          "exactness" => "projected_exact",
          "freshness" => "current",
          "coverage" => "complete",
          "pagination" => "complete"
        },
        "usage" => %{
          "range_seeks" => 0,
          "range_pages" => 0,
          "scanned_entries" => 0,
          "scanned_bytes" => 0,
          "hydrated_records" => 0,
          "residual_checks" => 0,
          "duplicate_entries" => 0,
          "result_records" => 0,
          "response_bytes" => 0,
          "memory_high_water_bytes" => 0,
          "wall_time_us" => 0
        }
      }
    end
  end

  @fields [:run_id, :state, {:attribute, "customer's.tier"}]
  @projection "RETURN RECORDS (run_id, state, attribute['customer''s.tier'])"

  test "every query collection helper supports a bounded sparse projection" do
    {:ok, client} = CaptureClient.start_link(self())

    calls = [
      fn ->
        Flow.list(client,
          type: "invoice",
          state: "queued",
          partition_key: "tenant-a",
          fields: @fields
        )
      end,
      fn ->
        Flow.search(client,
          type: "invoice",
          partition_key: "tenant-a",
          attributes: %{"customer" => "acme"},
          fields: @fields
        )
      end,
      fn -> Flow.terminals(client, "invoice", partition_key: "tenant-a", fields: @fields) end,
      fn -> Flow.failures(client, "invoice", partition_key: "tenant-a", fields: @fields) end,
      fn -> Flow.by_parent(client, "parent-1", partition_key: "tenant-a", fields: @fields) end,
      fn -> Flow.by_root(client, "root-1", partition_key: "tenant-a", fields: @fields) end,
      fn ->
        Flow.by_correlation(client, "correlation-1",
          partition_key: "tenant-a",
          fields: @fields
        )
      end,
      fn ->
        Flow.stuck(client, "invoice",
          partition_key: "tenant-a",
          now_ms: 1_000,
          fields: @fields
        )
      end
    ]

    Enum.each(calls, fn call ->
      assert [] = call.()
      assert_receive {:flow_query_payload, %{"query" => query}}
      assert String.ends_with?(query, @projection)
    end)
  end

  test "omitting fields preserves the complete-record return contract" do
    {:ok, client} = CaptureClient.start_link(self())

    assert [] =
             Flow.list(client,
               type: "invoice",
               state: "queued",
               partition_key: "tenant-a"
             )

    assert_receive {:flow_query_payload, %{"query" => query}}
    assert String.ends_with?(query, "RETURN RECORDS")
  end

  test "invalid convenience projections fail before transport" do
    invalid = [
      {[], :field_limit},
      {[:state, :state], :duplicate_field},
      {List.duplicate(:state, 33), :field_limit},
      {[{:event_field, "worker"}], :field_source},
      {[:not_a_public_run_field], :field_source},
      {[:state | :improper], :field_limit}
    ]

    Enum.each(invalid, fn {fields, reason} ->
      {:ok, client} = CaptureClient.start_link(self())

      assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_projection, ^reason}}} =
               Flow.list(client,
                 type: "invoice",
                 state: "queued",
                 partition_key: "tenant-a",
                 fields: fields
               )

      refute_received {:flow_query_payload, _payload}
    end)
  end
end
