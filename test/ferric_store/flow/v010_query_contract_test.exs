defmodule FerricStore.Flow.V010QueryContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow

  alias FerricStore.Flow.{
    QueryBuilder,
    QueryError,
    QueryExplainResult,
    QueryIndexStatus,
    QueryResponse,
    QueryResult
  }

  alias FerricStore.Flow.QueryRequest

  alias FerricStore.Protocol.{CapabilityContract, CommandSpec, Opcodes}
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.ServerContract
  alias FerricStore.Test.{ClientRuntime, NativeServer}

  @query "FROM runs WHERE partition_key = @tenant AND type = @type ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS"

  defmodule CaptureClient do
    use GenServer

    def start_link(owner, replies) do
      GenServer.start_link(__MODULE__, {owner, replies})
      |> ClientRuntime.wrap()
    end

    @impl true
    def init({owner, replies}), do: {:ok, %{owner: owner, replies: replies}}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, state) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, state)
    end

    def handle_call({kind, opcode, payload, %RequestContext{}}, _from, state)
        when kind in [:request, :command] do
      send(state.owner, {:native_request, opcode, payload})
      [reply | rest] = state.replies
      {:reply, reply, %{state | replies: rest}}
    end

    def handle_call({:command_exec, command, args, %RequestContext{}}, _from, state) do
      send(state.owner, {:command_exec, command, args})
      [reply | rest] = state.replies
      {:reply, reply, %{state | replies: rest}}
    end
  end

  test "0.10 exposes only the FQL collection opcode and schema" do
    assert Opcodes.flow_query() == 0x0231
    assert CommandSpec.read_only?(:flow_query)

    for removed <- ~w(
          FLOW.LIST FLOW.SEARCH FLOW.TERMINALS FLOW.FAILURES FLOW.STUCK
          FLOW.BY_PARENT FLOW.BY_ROOT FLOW.BY_CORRELATION
        ) do
      assert CommandSpec.fetch(removed) == :error
    end

    assert CapabilityContract.required_schemas()["FLOW.QUERY"] == ["version", "query"]

    assert CapabilityContract.required_schema_fields()["FLOW.QUERY"] == [
             "version",
             "query",
             "params",
             "deadline_ms"
           ]

    for module <- [Flow, FerricStore.Flow.Payload, FerricStore.Flow.Payload.Query] do
      refute function_exported?(module, :list_payload, 1)
      refute function_exported?(module, :search_payload, 1)
    end
  end

  test "HELLO requires the complete query capability manifest" do
    assert :ok = ServerContract.validate(NativeServer.startup_payload())

    startup =
      NativeServer.startup_payload(%{
        "capabilities" => %{"flow_query" => %{"shapes" => ["runs_by_run_id_record"]}}
      })

    assert {:error,
            {:incompatible_server_contract, %{flow_query: "shapes", missing: missing_shapes}}} =
             ServerContract.validate(startup)

    assert "runs_by_partition_predicates_ordered_records" in missing_shapes

    incompatible_index_status =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "flow_query" => %{"index_status_contract" => "future.flow.query.indexes/v2"}
        }
      })

    assert {:error,
            {:incompatible_server_contract,
             %{
               flow_query: "index_status_contract",
               expected: "ferric.flow.query.indexes/v1"
             }}} = ServerContract.validate(incompatible_index_status)
  end

  test "query sends the bounded native request and decodes the versioned page" do
    {:ok, client} = CaptureClient.start_link(self(), [{:ok, query_response()}])

    assert %QueryResult{
             version: "ferric.flow.query.result/v1",
             records: [%{"id" => "one"}, %{"id" => "two"}],
             count: nil,
             page: %{has_more: true, cursor: "fqc1_page"},
             usage: %{result_records: 2}
           } = Flow.query(client, @query, %{"type" => "invoice", "tenant" => "tenant-a"})

    assert_received {:native_request, 0x0231,
                     %{
                       "version" => "FQL1",
                       "query" => @query,
                       "params" => %{"type" => "invoice", "tenant" => "tenant-a"}
                     }}
  end

  test "query validates its bounded input before transport" do
    {:ok, client} = CaptureClient.start_link(self(), [])

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query, :empty_query}}} =
             Flow.query(client, "", %{})

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_parameter, "bad", :type}}} =
             Flow.query(client, "FROM runs WHERE run_id = @bad RETURN RECORD", %{
               "bad" => self()
             })

    refute_received {:native_request, _, _}
  end

  test "query text identifiers reject invalid UTF-8 without raising or transport" do
    invalid = <<255>>

    assert {:error, {:invalid_flow_query, :invalid_utf8}} = QueryRequest.payload(invalid, %{})

    assert {:error, {:invalid_flow_query_parameter, ^invalid, :name}} =
             QueryRequest.payload(
               "FROM runs WHERE run_id = @id RETURN RECORD",
               %{invalid => "one"}
             )

    {:ok, client} = CaptureClient.start_link(self(), [])

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_index_id, ^invalid}}} =
             Flow.query_indexes(client, invalid)

    refute_received {:native_request, _, _}
    refute_received {:command_exec, _, _}
  end

  test "query preserves actionable structured diagnostics" do
    diagnostic = %{
      "code" => "unsupported_field",
      "message" => "unsupported query field",
      "detail" => "Use a supported field.",
      "hint" => "See context.supported_fields.",
      "retryable" => false,
      "safe_to_retry" => false,
      "retry_after_ms" => 0,
      "position" => %{"byte" => 18, "line" => 1, "column" => 19},
      "context" => %{"supported_fields" => ["partition_key", "run_id", "type"]}
    }

    {:ok, client} = CaptureClient.start_link(self(), [{:error, {:bad_request, diagnostic}}])

    assert {:error,
            %QueryError{
              code: "unsupported_field",
              position: %{byte: 18, line: 1, column: 19},
              context: %{"supported_fields" => ["partition_key", "run_id", "type"]},
              raw: {:bad_request, ^diagnostic}
            }} = Flow.query(client, "FROM runs WHERE nope = 1 RETURN RECORD")
  end

  test "malformed diagnostics fail closed as their original transport error" do
    raw = {:bad_request, %{"code" => "unsupported_field"}}
    {:ok, client} = CaptureClient.start_link(self(), [{:error, raw}])

    assert {:error,
            %FerricStore.Error{
              status: :bad_request,
              raw: %{"code" => "unsupported_field"}
            }} =
             Flow.query(client, "FROM runs WHERE run_id = @id RETURN RECORD", %{"id" => "one"})
  end

  test "search rejects empty prepared metadata instead of issuing a broad query" do
    {:ok, client} = CaptureClient.start_link(self(), [])

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_query_option, :missing_metadata_predicate}
            }} =
             Flow.search(client,
               type: "invoice",
               partition_key: "tenant-a",
               attributes: %{}
             )

    refute_received {:native_request, _, _}
  end

  test "collection builders preserve server metadata normalization" do
    assert {:ok, query, _params} =
             QueryBuilder.search(
               type: "invoice",
               partition_key: "tenant-a",
               attributes: %{" customer " => "one"},
               state_meta: %{" queued " => %{" risk " => 3}}
             )

    assert query =~ "attribute['customer'] = @attribute_0"
    assert query =~ "state_meta['queued']['risk'] = @state_meta_0"
  end

  test "collection builders reject invalid normalized metadata" do
    for attributes <- [
          %{"tenant" => "one", " tenant " => "two"},
          %{"__internal" => "one"},
          %{String.duplicate("x", 65) => "one"}
        ] do
      assert {:error, {:invalid_flow_query_option, :attributes}} =
               QueryBuilder.search(
                 type: "invoice",
                 partition_key: "tenant-a",
                 attributes: attributes
               )
    end

    assert {:error, {:invalid_flow_query_option, :state_meta}} =
             QueryBuilder.search(
               type: "invoice",
               partition_key: "tenant-a",
               state_meta: %{"queued" => %{"risk" => 1}, " queued " => %{"risk" => 2}}
             )
  end

  test "collection builders enforce the exact timestamp domain" do
    maximum = 9_007_199_254_740_991

    assert {:ok, _query, %{"from_ms" => ^maximum, "to_ms" => ^maximum}} =
             QueryBuilder.list(
               type: "invoice",
               partition_key: "tenant-a",
               from_ms: maximum,
               to_ms: maximum
             )

    assert {:error, {:invalid_flow_query_option, :time_window}} =
             QueryBuilder.list(
               type: "invoice",
               partition_key: "tenant-a",
               to_ms: maximum + 1
             )
  end

  test "collection builders reject query shapes with no bounded OSS plan" do
    assert {:error, {:invalid_flow_query_option, :bounded_source}} =
             QueryBuilder.list(type: "any", partition_key: "tenant-a")

    assert {:error, {:invalid_flow_query_option, :bounded_source}} =
             QueryBuilder.list(type: "invoice", state: "any", partition_key: "tenant-a")

    assert {:error, {:invalid_flow_query_option, :state_meta_requires_type}} =
             QueryBuilder.search(
               type: "any",
               partition_key: "tenant-a",
               state_meta: %{"queued" => %{"risk" => 3}}
             )

    assert {:ok, query, _params} =
             QueryBuilder.list(
               type: "any",
               partition_key: "tenant-a",
               attributes: %{"tenant" => "acme"}
             )

    assert query =~ "attribute['tenant'] = @attribute_0"

    assert {:ok, atom_any_query, _params} =
             QueryBuilder.list(
               type: "invoice",
               state: :any,
               partition_key: "tenant-a",
               attributes: %{"tenant" => "acme"}
             )

    refute atom_any_query =~ "state = @state"

    assert {:ok, terminal_query, _params} =
             QueryBuilder.terminals(
               type: "invoice",
               state: :any,
               partition_key: "tenant-a"
             )

    assert terminal_query =~ "state IN (@terminal_0, @terminal_1, @terminal_2)"

    assert {:error, {:unsupported_flow_query_option, :attributes}} =
             QueryBuilder.terminals(
               type: "invoice",
               partition_key: "tenant-a",
               attributes: %{"tenant" => "acme"}
             )
  end

  test "removed collection opcodes have complete bounded FQL conveniences" do
    {:ok, client} =
      CaptureClient.start_link(self(), List.duplicate({:ok, query_response()}, 6))

    records = [%{"id" => "one"}, %{"id" => "two"}]

    assert ^records = Flow.terminals(client, "invoice", partition_key: "tenant-a", count: 2)
    assert ^records = Flow.failures(client, "invoice", partition_key: "tenant-a", count: 2)
    assert ^records = Flow.by_parent(client, "parent-1", partition_key: "tenant-a", count: 2)
    assert ^records = Flow.by_root(client, "root-1", partition_key: "tenant-a", count: 2)

    assert ^records =
             Flow.by_correlation(client, "correlation-1",
               partition_key: "tenant-a",
               count: 2
             )

    assert ^records =
             Flow.stuck(client, "invoice",
               partition_key: "tenant-a",
               count: 2,
               older_than_ms: 100,
               now_ms: 1_000
             )

    queries =
      for _index <- 1..6 do
        assert_receive {:native_request, 0x0231, %{"query" => query}}
        query
      end

    assert Enum.at(queries, 0) =~ "state IN (@terminal_0, @terminal_1, @terminal_2)"
    assert Enum.at(queries, 1) =~ "state = @state"
    assert Enum.at(queries, 2) =~ "parent_flow_id = @lineage_id"
    assert Enum.at(queries, 3) =~ "root_flow_id = @lineage_id"
    assert Enum.at(queries, 4) =~ "correlation_id = @lineage_id"
    assert Enum.at(queries, 5) =~ "ORDER BY lease_deadline_ms ASC LIMIT 2 RETURN RECORDS"
  end

  test "convenience helpers reject unsupported planner shapes before transport" do
    {:ok, client} = CaptureClient.start_link(self(), [])

    assert {:error, %FerricStore.Error{}} =
             Flow.terminals(client, "any", partition_key: "tenant-a")

    assert {:error, %FerricStore.Error{}} =
             Flow.by_parent(client, "parent-1",
               partition_key: "tenant-a",
               attributes: %{"tenant" => "acme"}
             )

    assert {:error, %FerricStore.Error{}} =
             Flow.stuck(client, "any", partition_key: "tenant-a", now_ms: 1_000)

    assert {:error, %FerricStore.Error{}} =
             Flow.search(client,
               type: "invoice",
               state: "any",
               partition_key: "tenant-a",
               state_meta: %{risk: 3}
             )

    refute_received {:native_request, _, _}
  end

  test "explain, analyze, count and index status keep distinct response contracts" do
    {:ok, client} =
      CaptureClient.start_link(self(), [
        {:ok, explain_response("planned", nil)},
        {:ok, explain_response("executed", usage(2))},
        {:ok, count_response(3)},
        {:ok, index_status_response()}
      ])

    assert %QueryExplainResult{status: "planned", actual: nil} =
             Flow.explain(client, @query, %{"tenant" => "tenant-a", "type" => "invoice"})

    assert %QueryExplainResult{status: "executed", actual: %{result_records: 2}} =
             Flow.explain_analyze(client, @query, %{
               "tenant" => "tenant-a",
               "type" => "invoice"
             })

    assert %QueryResult{records: nil, count: 3, page: nil} =
             Flow.query(
               client,
               "FROM runs WHERE partition_key = @tenant AND type = @type RETURN COUNT",
               %{"tenant" => "tenant-a", "type" => "invoice"}
             )

    assert %QueryIndexStatus{
             contract_version: "ferric.flow.query.indexes/v1",
             registry: %{catalog_version: 1},
             indexes: [%{id: "flow_runs_tenant_updated", queryable: true}]
           } = Flow.query_indexes(client)

    assert_received {:native_request, 0x0231, %{"query" => "EXPLAIN " <> @query}}
    assert_received {:native_request, 0x0231, %{"query" => "EXPLAIN ANALYZE " <> @query}}
    assert_received {:native_request, 0x0100, %{"command" => "FLOW.QUERY.INDEXES", "args" => []}}
  end

  test "explain rejects a malformed query fingerprint" do
    malformed = Map.put(explain_response("planned", nil), "query_fingerprint", "abc123")

    assert {:error, {:invalid_flow_query_response, :query_fingerprint, "abc123"}} =
             QueryResponse.explain(malformed)
  end

  test "index status accepts exactly the unsigned 64-bit metadata domain" do
    maximum = 18_446_744_073_709_551_615

    assert {:ok,
            %QueryIndexStatus{
              registry: %{epoch: ^maximum, catalog_version: ^maximum},
              indexes: [%{version: ^maximum}]
            }} = QueryResponse.indexes(index_status_response(maximum))

    invalid = put_in(index_status_response(), ["registry", "epoch"], maximum + 1)

    assert {:error, {:invalid_flow_query_response, {:unsigned, "epoch"}, _value}} =
             QueryResponse.indexes(invalid)
  end

  test "query counters remain in the signed 64-bit domain" do
    maximum = 9_223_372_036_854_775_807

    assert {:ok, %QueryResult{count: ^maximum}} =
             QueryResponse.result(count_response(maximum))

    assert {:error, {:invalid_flow_query_response, :count, _reason}} =
             QueryResponse.result(count_response(maximum + 1))

    invalid_usage = put_in(query_response(), ["usage", "scanned_entries"], maximum + 1)

    assert {:error, {:invalid_flow_query_response, {:non_negative, "scanned_entries"}, _value}} =
             QueryResponse.result(invalid_usage)

    invalid_status = put_in(index_status_response(), ["observed_at_ms"], maximum + 1)

    assert {:error, {:invalid_flow_query_response, {:non_negative, "observed_at_ms"}, _value}} =
             QueryResponse.indexes(invalid_status)
  end

  test "query responses reject invalid UTF-8 text" do
    invalid = put_in(query_response(), ["quality", "exactness"], <<0xFF>>)

    assert {:error, {:invalid_flow_query_response, {:binary, "exactness"}, <<0xFF>>}} =
             QueryResponse.result(invalid)
  end

  test "query responses reject oversized quality text" do
    invalid = put_in(query_response(), ["quality", "exactness"], String.duplicate("x", 65))

    assert {:error, {:invalid_flow_query_response, {:binary, "exactness"}, _value}} =
             QueryResponse.result(invalid)
  end

  test "list convenience compiles FQL instead of probing the removed opcode" do
    {:ok, client} = CaptureClient.start_link(self(), [{:ok, query_response()}])

    assert [%{"id" => "one"}, %{"id" => "two"}] =
             Flow.list(client,
               type: "invoice",
               state: "failed",
               partition_key: "tenant-a",
               count: 2,
               rev: true,
               return: :meta
             )

    assert_received {:native_request, 0x0231,
                     %{
                       "query" => query,
                       "params" => %{
                         "partition_key" => "tenant-a",
                         "state" => "failed",
                         "type" => "invoice"
                       }
                     }}

    assert query ==
             "FROM runs WHERE partition_key = @partition_key AND type = @type AND state = @state ORDER BY updated_at_ms DESC LIMIT 2 RETURN RECORDS"
  end

  defp query_response do
    %{
      "version" => "ferric.flow.query.result/v1",
      "records" => [%{"id" => "one"}, %{"id" => "two"}],
      "page" => %{"has_more" => true, "cursor" => "fqc1_page"},
      "quality" => quality(),
      "usage" => usage(2)
    }
  end

  defp count_response(count) do
    %{
      "version" => "ferric.flow.query.result/v1",
      "result" => %{"kind" => "count", "value" => count},
      "quality" => quality(),
      "usage" => usage(1)
    }
  end

  defp explain_response(status, actual) do
    %{
      "version" => "ferric.flow.explain/v1",
      "query_fingerprint" => String.duplicate("a", 64),
      "status" => status,
      "plan" => %{"path" => "composite"},
      "estimate" => %{"scanned_entries" => 2},
      "bounds" => %{"scanned_entries" => 50_000},
      "actual" => actual
    }
  end

  defp quality do
    %{
      "exactness" => "exact",
      "freshness" => "authoritative",
      "coverage" => "complete",
      "pagination" => "stable"
    }
  end

  defp usage(result_records) do
    %{
      "range_seeks" => 1,
      "range_pages" => 1,
      "scanned_entries" => result_records,
      "scanned_bytes" => 100,
      "hydrated_records" => result_records,
      "residual_checks" => 0,
      "duplicate_entries" => 0,
      "result_records" => result_records,
      "response_bytes" => 100,
      "memory_high_water_bytes" => 1_024,
      "wall_time_us" => 10
    }
  end

  defp index_status_response(version \\ 1) do
    %{
      "contract_version" => "ferric.flow.query.indexes/v1",
      "observed_at_ms" => 1,
      "statistics_max_age_ms" => 10_000,
      "registry" => %{"epoch" => version, "catalog_version" => version},
      "services" => %{"projection" => %{"available" => true}},
      "indexes" => [
        %{
          "id" => "flow_runs_tenant_updated",
          "version" => version,
          "build_id" => "build-1",
          "state" => "active",
          "queryable" => true
        }
      ]
    }
  end
end
