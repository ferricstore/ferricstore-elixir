defmodule FerricStore.Flow.V010QueryContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow

  alias FerricStore.Flow.{
    QueryBuilder,
    QueryError,
    QueryExplainCapabilities,
    QueryExplainResult,
    QueryIndex,
    QueryIndexBuild,
    QueryIndexCoverage,
    QueryIndexField,
    QueryIndexFormat,
    QueryIndexRetirement,
    QueryIndexServices,
    QueryIndexStatistics,
    QueryIndexStatus,
    QueryIndexValidation,
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

    missing_projection =
      NativeServer.startup_payload(%{
        "capabilities" => %{
          "flow_query" => %{
            "capabilities" => [
              "flow_query_v1",
              "flow_explain_v1",
              "flow_explain_analyze_v1",
              "flow_composite_index_v1",
              "flow_query_index_status_v1"
            ]
          }
        }
      })

    assert {:error,
            {:incompatible_server_contract,
             %{flow_query: "capabilities", missing: ["flow_query_result_projection_v1"]}}} =
             ServerContract.validate(missing_projection)

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
             page: %{has_more: true, cursor: "fqc1_next-page-token"},
             usage: %{result_records: 2}
           } = Flow.query(client, @query, %{"type" => "invoice", "tenant" => "tenant-a"})

    assert_received {:native_request, 0x0231,
                     %{
                       "version" => "FQL1",
                       "query" => @query,
                       "params" => %{"type" => "invoice", "tenant" => "tenant-a"}
                     }}
  end

  test "EXPLAIN preserves non-grammar leading whitespace" do
    query = "\u00A0FROM runs WHERE run_id = @id RETURN RECORD"
    {:ok, client} = CaptureClient.start_link(self(), [{:ok, explain_response("planned", nil)}])

    assert %QueryExplainResult{status: "planned"} =
             Flow.explain(client, query, %{"id" => "run-1"})

    assert_received {:native_request, 0x0231, %{"query" => "EXPLAIN " <> ^query}}
  end

  test "query preserves sparse records returned by a field projection" do
    response = %{
      query_response()
      | "records" => [
          %{"id" => "one", "state" => "queued", "attributes" => %{"customer" => "acme"}}
        ],
        "page" => %{"has_more" => false, "cursor" => nil},
        "usage" => usage(1)
    }

    {:ok, client} = CaptureClient.start_link(self(), [{:ok, response}])

    query =
      "FROM runs WHERE run_id = @run RETURN RECORD " <>
        "(run_id, state, attribute['customer'])"

    assert %QueryResult{records: [record]} = Flow.query(client, query, %{"run" => "one"})

    assert record == %{
             "id" => "one",
             "state" => "queued",
             "attributes" => %{"customer" => "acme"}
           }
  end

  test "query validates its bounded input before transport" do
    {:ok, client} = CaptureClient.start_link(self(), [])

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query, :empty_query}}} =
             Flow.query(client, "", %{})

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_parameter, "bad", :type}}} =
             Flow.query(client, "FROM runs WHERE run_id = @bad RETURN RECORD", %{
               "bad" => self()
             })

    for name <- ["bad name", "unicode_ä", "bad:name", "bad/name", "bad\n"] do
      assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_parameter, ^name, :name}}} =
               Flow.query(client, "FROM runs WHERE run_id = @id RETURN RECORD", %{name => "one"})
    end

    for value <- [String.duplicate("x", 65_536), :binary.copy(<<0>>, 65_536)] do
      assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_parameter, "id", :size}}} =
               Flow.query(client, "FROM runs WHERE run_id = @id RETURN RECORD", %{"id" => value})
    end

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

    trailing_newline = "index\n"

    assert {:error, %FerricStore.Error{raw: {:invalid_flow_query_index_id, ^trailing_newline}}} =
             Flow.query_indexes(client, trailing_newline)

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

  test "diagnostics enforce the server's bounded wire contract" do
    diagnostic = query_diagnostic_response()

    too_deep =
      Enum.reduce(~w(g f e d c b a), 1, fn key, nested -> %{key => nested} end)

    malformed = [
      Map.put(diagnostic, "detail", String.duplicate("x", 1_025)),
      Map.put(
        diagnostic,
        "context",
        Map.new(0..16, fn index -> {"field_#{index}", index} end)
      ),
      Map.put(diagnostic, "context", %{0 => "invalid"}),
      Map.put(diagnostic, "context", %{"" => "invalid"}),
      Map.put(diagnostic, "context", %{"fields" => List.duplicate(0, 33)}),
      Map.put(diagnostic, "context", %{"estimate" => 1.5}),
      Map.put(diagnostic, "context", %{"estimate" => 0x8000_0000_0000_0000}),
      Map.put(diagnostic, "context", too_deep)
    ]

    assert {:ok, %QueryError{code: "unsupported_field"}} =
             QueryResponse.diagnostic({:bad_request, diagnostic})

    for invalid <- malformed do
      assert :error = QueryResponse.diagnostic({:bad_request, invalid})
    end
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

    for query <- Enum.take(queries, 5) do
      assert query =~ "ORDER BY updated_at_ms DESC"
    end

    assert Enum.at(queries, 5) =~ "ORDER BY lease_deadline_ms ASC LIMIT 2 RETURN RECORDS"
  end

  test "collection builders default to index-native updated-time order" do
    assert {:ok, default_query, _params} =
             QueryBuilder.list(
               type: "invoice",
               state: "queued",
               partition_key: "tenant-a",
               count: 25
             )

    assert default_query =~ "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"

    assert {:ok, nil_reverse_query, _params} =
             QueryBuilder.list(
               type: "invoice",
               state: "queued",
               partition_key: "tenant-a",
               count: 25,
               rev: nil
             )

    assert nil_reverse_query =~ "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"

    assert {:ok, ascending_query, _params} =
             QueryBuilder.list(
               type: "invoice",
               state: "queued",
               partition_key: "tenant-a",
               count: 25,
               rev: false
             )

    assert ascending_query =~ "ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS"

    assert {:ok, stuck_query, _params} =
             QueryBuilder.stuck(
               type: "invoice",
               partition_key: "tenant-a",
               count: 25,
               now_ms: 1_000,
               older_than_ms: 100
             )

    assert stuck_query =~ "ORDER BY lease_deadline_ms ASC LIMIT 25 RETURN RECORDS"
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

    assert %QueryExplainResult{
             status: "planned",
             actual: nil,
             stats: %{"source" => "fresh"},
             quality: %{pagination: "live_seek"},
             decision: %{"reason" => "only_bounded_candidate"},
             alternatives: []
           } =
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
             indexes: [
               %QueryIndex{
                 id: "flow_runs_tenant_updated",
                 source: "runs",
                 queryable: true,
                 fields: [
                   %QueryIndexField{name: "partition_key", direction: "asc", encoding: "hashed"},
                   %QueryIndexField{name: "updated_at_ms", direction: "desc", encoding: "ordered"}
                 ],
                 workloads: ["tenant_updated"],
                 count_prefixes: [1],
                 covering_fields: [
                   "partition_key",
                   "run_id",
                   "updated_at_ms",
                   "version"
                 ],
                 format: %QueryIndexFormat{
                   entry: "ferric.flow.query.composite.entry/v2",
                   counter: "ferric.flow.query.composite.counter/v1"
                 },
                 coverage: %QueryIndexCoverage{validation: "passed"},
                 build: %QueryIndexBuild{scanned_records: 10},
                 validation: %QueryIndexValidation{status: "passed", validated_at_ms: 999_000},
                 retirement: %QueryIndexRetirement{status: "not_applicable"},
                 statistics: %QueryIndexStatistics{status: "fresh", samples: 2}
               }
             ],
             services: %QueryIndexServices{statistics_store: "ready"}
           } = Flow.query_indexes(client)

    assert_received {:native_request, 0x0231, %{"query" => "EXPLAIN " <> @query}}
    assert_received {:native_request, 0x0231, %{"query" => "EXPLAIN ANALYZE " <> @query}}
    assert_received {:native_request, 0x0100, %{"command" => "FLOW.QUERY.INDEXES", "args" => []}}
  end

  test "0.11 index status requires bounded covering and format metadata" do
    mutations = [
      &pop_in(&1, ["indexes", Access.at(0), "covering_fields"]),
      &put_in(&1, ["indexes", Access.at(0), "covering_fields"], ["run_id", "run_id"]),
      &put_in(
        &1,
        ["indexes", Access.at(0), "covering_fields"],
        Enum.map(0..32, fn position -> "attribute.field_#{position}" end)
      ),
      &pop_in(&1, ["indexes", Access.at(0), "format"]),
      &put_in(&1, ["indexes", Access.at(0), "format", "counter"], false)
    ]

    for mutate <- mutations do
      malformed =
        case mutate.(index_status_response()) do
          {_removed, response} -> response
          response -> response
        end

      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.indexes(malformed)
    end
  end

  test "EXPLAIN requires the complete actionable v1 envelope" do
    for field <- ~w(stats quality pressure decision alternatives actual diagnostic) do
      malformed = Map.delete(explain_response("planned", nil), field)

      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.explain(malformed)
    end
  end

  test "EXPLAIN decodes bounded specialized executor capabilities" do
    response = specialized_explain_response()

    assert {:ok,
            %QueryExplainResult{
              status: "planned",
              stats: nil,
              quality: nil,
              pressure: nil,
              decision: nil,
              alternatives: [],
              actual: nil,
              diagnostic: nil,
              capabilities: %QueryExplainCapabilities{
                requested: ["flow_query_point_v1"],
                available: ["flow_query_point_v1", "flow_query_history_v1"],
                missing: []
              }
            }} = QueryResponse.explain(response)
  end

  test "EXPLAIN rejects malformed specialized executor envelopes" do
    response = specialized_explain_response()

    malformed = [
      Map.delete(response, "capabilities"),
      pop_in(response, ["capabilities", "requested"]) |> elem(1),
      put_in(response, ["capabilities", "available"], [
        "flow_query_point_v1",
        "flow_query_point_v1"
      ]),
      put_in(
        response,
        ["capabilities", "missing"],
        Enum.map(0..64, &"missing_#{&1}")
      ),
      Map.put(response, "stats", %{}),
      Map.put(response, "status", "executed"),
      Map.put(response, "actual", nil)
    ]

    for invalid <- malformed do
      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.explain(invalid)
    end
  end

  test "index status requires every lifecycle section and service" do
    for field <-
          ~w(source fields workloads count_prefixes coverage build validation retirement statistics) do
      malformed = pop_in(index_status_response(), ["indexes", Access.at(0), field]) |> elem(1)

      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.indexes(malformed)
    end

    for service <- ~w(registry lifecycle_worker statistics_store statistics_worker) do
      malformed = pop_in(index_status_response(), ["services", service]) |> elem(1)

      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.indexes(malformed)
    end
  end

  test "index status decodes retirement progress without build scope" do
    response =
      index_status_response()
      |> put_in(["indexes", Access.at(0), "state"], "retiring")
      |> put_in(["indexes", Access.at(0), "queryable"], false)
      |> put_in(
        ["indexes", Access.at(0), "retirement"],
        %{
          "status" => "pending",
          "phase_counts" => %{"pending" => 2},
          "current_phases" => ["pending"],
          "completed_shards" => 0,
          "total_shards" => 2,
          "deleted_entries" => 0,
          "deleted_bytes" => 0,
          "rewritten_reverse_rows" => 0
        }
      )

    assert {:ok,
            %QueryIndexStatus{
              indexes: [%QueryIndex{retirement: %QueryIndexRetirement{status: "pending"}}]
            }} = QueryResponse.indexes(response)
  end

  test "filtered index status rejects a different identity" do
    response = put_in(index_status_response(), ["indexes", Access.at(0), "id"], "different_index")

    assert {:error, {:invalid_flow_query_response, {:index_contract, :filtered_identity}, _raw}} =
             QueryResponse.indexes(response, "flow_runs_tenant_updated")
  end

  test "index status rejects cross-field lifecycle contradictions" do
    malformed = [
      put_in(index_status_response(), ["indexes", Access.at(0), "id"], "index\n"),
      put_in(index_status_response(), ["indexes", Access.at(0), "source"], "events"),
      put_in(
        index_status_response(),
        ["indexes", Access.at(0), "fields", Access.at(0), "name"],
        "type"
      ),
      put_in(index_status_response(), ["indexes", Access.at(0), "count_prefixes"], []),
      put_in(index_status_response(), ["indexes", Access.at(0), "validation", "total_shards"], 3),
      put_in(index_status_response(), ["indexes", Access.at(0), "queryable"], false),
      put_in(index_status_response(), ["indexes", Access.at(0), "validation", "mismatches"], 1),
      update_in(
        index_status_response(),
        ["indexes", Access.at(0), "covering_fields"],
        &(&1 ++ ["attribute.customer\n"])
      ),
      put_in(
        index_status_response(),
        ["indexes", Access.at(0), "statistics", "fresh_samples"],
        1
      ),
      put_in(index_status_response(), ["indexes", Access.at(0), "statistics", "newest_age_ms"], 2)
    ]

    for response <- malformed do
      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.indexes(response)
    end
  end

  test "explain rejects a malformed query fingerprint" do
    malformed = Map.put(explain_response("planned", nil), "query_fingerprint", "abc123")

    assert {:error, {:invalid_flow_query_response, :query_fingerprint, "abc123"}} =
             QueryResponse.explain(malformed)
  end

  test "index status accepts exactly the unsigned 64-bit metadata domain" do
    maximum = 18_446_744_073_709_551_615

    response =
      index_status_response(maximum)
      |> put_in(["observed_at_ms"], maximum)
      |> put_in(["statistics_max_age_ms"], maximum)
      |> put_in(["indexes", Access.at(0), "state"], "retiring")
      |> put_in(["indexes", Access.at(0), "queryable"], false)
      |> put_in(["indexes", Access.at(0), "coverage", "complete_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "coverage", "total_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "build", "phase_counts"], %{"done" => maximum})
      |> put_in(["indexes", Access.at(0), "build", "completed_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "build", "total_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "build", "scanned_records"], maximum)
      |> put_in(["indexes", Access.at(0), "build", "written_entries"], maximum)
      |> put_in(["indexes", Access.at(0), "build", "written_bytes"], maximum)
      |> put_in(["indexes", Access.at(0), "validation", "phase_counts"], %{
        "done" => maximum
      })
      |> put_in(["indexes", Access.at(0), "validation", "completed_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "validation", "total_shards"], maximum)
      |> put_in(["indexes", Access.at(0), "validation", "checked_records"], maximum)
      |> put_in(["indexes", Access.at(0), "validation", "checked_entries"], maximum)
      |> put_in(["indexes", Access.at(0), "validation", "validated_at_ms"], maximum)
      |> put_in(
        ["indexes", Access.at(0), "retirement"],
        %{
          "status" => "complete",
          "phase_counts" => %{"done" => maximum},
          "current_phases" => ["done"],
          "completed_shards" => maximum,
          "total_shards" => maximum,
          "deleted_entries" => maximum,
          "deleted_bytes" => maximum,
          "rewritten_reverse_rows" => maximum
        }
      )
      |> put_in(["indexes", Access.at(0), "statistics"], %{
        "status" => "fresh",
        "samples" => maximum,
        "fresh_samples" => maximum,
        "stale_samples" => 0,
        "future_samples" => 0,
        "oldest_collected_at_ms" => 0,
        "newest_collected_at_ms" => 0,
        "oldest_age_ms" => maximum,
        "newest_age_ms" => maximum
      })

    assert {:ok,
            %QueryIndexStatus{
              observed_at_ms: ^maximum,
              registry: %{epoch: ^maximum, catalog_version: ^maximum},
              indexes: [
                %{
                  version: ^maximum,
                  build: %{scanned_records: ^maximum},
                  validation: %{validated_at_ms: ^maximum},
                  retirement: %{deleted_entries: ^maximum},
                  statistics: %{samples: ^maximum}
                }
              ]
            }} = QueryResponse.indexes(response)

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

  test "query responses reject unsupported quality values" do
    invalid = put_in(query_response(), ["quality", "exactness"], "future_exactness")

    assert {:error, {:invalid_flow_query_response, {:quality, "exactness"}, _value}} =
             QueryResponse.result(invalid)
  end

  test "query response page validation is bounded and preserves error precedence" do
    assert {:ok, %QueryResult{page: %{has_more: false, cursor: nil}}} =
             QueryResponse.result(put_in(query_response(), ["page"], %{"has_more" => false}))

    cases = [
      {%{"has_more" => "true", "cursor" => <<0xFF>>}, :page_has_more, "true"},
      {%{"has_more" => false, "cursor" => <<0xFF>>}, :page_cursor, <<0xFF>>},
      {%{"has_more" => false, "cursor" => String.duplicate("x", 4_097)}, :page_cursor,
       String.duplicate("x", 4_097)},
      {%{"has_more" => true, "cursor" => "other_cursor"}, :page_cursor, "other_cursor"},
      {%{"has_more" => true, "cursor" => nil}, :page_consistency,
       %{"has_more" => true, "cursor" => nil}},
      {%{"has_more" => true, "cursor" => "fqc1_short"}, :page_cursor, "fqc1_short"},
      {%{"has_more" => false, "cursor" => "fqc1_next-page-token"}, :page_consistency,
       %{"has_more" => false, "cursor" => "fqc1_next-page-token"}}
    ]

    for {page, field, value} <- cases do
      assert {:error,
              {:invalid_flow_query_response, :records,
               {:invalid_flow_query_response, ^field, ^value}}} =
               QueryResponse.result(put_in(query_response(), ["page"], page))
    end
  end

  test "query response usage counters preserve server invariants" do
    cases = [
      {"hydrated_records", 3},
      {"duplicate_entries", 3},
      {"range_pages", 4},
      {"residual_checks", 25},
      {"scanned_entries", 1}
    ]

    for {field, value} <- cases do
      malformed = put_in(query_response(), ["usage", field], value)

      assert {:error, {:invalid_flow_query_response, _field, _value}} =
               QueryResponse.result(malformed)
    end
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
      "page" => %{"has_more" => true, "cursor" => "fqc1_next-page-token"},
      "quality" => Map.put(quality(), "pagination", "live_seek"),
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
      "stats" => %{"source" => "fresh"},
      "quality" => Map.put(quality(), "pagination", "live_seek"),
      "bounds" => %{"scanned_entries" => 50_000},
      "pressure" => %{"resources" => []},
      "decision" => %{"reason" => "only_bounded_candidate"},
      "alternatives" => [],
      "actual" => actual,
      "diagnostic" => nil
    }
  end

  defp specialized_explain_response do
    %{
      "version" => "ferric.flow.explain/v1",
      "query_fingerprint" => String.duplicate("b", 64),
      "status" => "planned",
      "plan" => %{"path" => "point_lookup"},
      "estimate" => %{"scanned_entries" => 1},
      "bounds" => %{"scanned_entries" => 1},
      "capabilities" => %{
        "requested" => ["flow_query_point_v1"],
        "available" => ["flow_query_point_v1", "flow_query_history_v1"],
        "missing" => []
      }
    }
  end

  defp query_diagnostic_response do
    %{
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
  end

  defp quality do
    %{
      "exactness" => "authoritative",
      "freshness" => "current",
      "coverage" => "complete",
      "pagination" => "authenticated_seek"
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
      "observed_at_ms" => 1_000_000,
      "statistics_max_age_ms" => 300_000,
      "registry" => %{"epoch" => version, "catalog_version" => version},
      "services" => %{
        "registry" => "ready",
        "lifecycle_worker" => "ready",
        "statistics_store" => "ready",
        "statistics_worker" => "unavailable"
      },
      "indexes" => [
        %{
          "id" => "flow_runs_tenant_updated",
          "version" => version,
          "build_id" => "build-1",
          "source" => "runs",
          "state" => "active",
          "queryable" => true,
          "fields" => [
            %{"name" => "partition_key", "direction" => "asc", "encoding" => "hashed"},
            %{"name" => "updated_at_ms", "direction" => "desc", "encoding" => "ordered"}
          ],
          "workloads" => ["tenant_updated"],
          "count_prefixes" => [1],
          "covering_fields" => [
            "partition_key",
            "run_id",
            "updated_at_ms",
            "version"
          ],
          "format" => %{
            "query_row" => "ferric.flow.query.row/v1",
            "key" => "ferric.flow.query.composite.key/v1",
            "entry" => "ferric.flow.query.composite.entry/v2",
            "reverse" => "ferric.flow.query.composite.reverse/v1",
            "counter" => "ferric.flow.query.composite.counter/v1"
          },
          "coverage" => %{
            "complete_shards" => 2,
            "total_shards" => 2,
            "validation" => "passed"
          },
          "build" => %{
            "scope" => "catalog_build",
            "phase_counts" => %{"done" => 2},
            "current_phases" => ["done"],
            "completed_shards" => 2,
            "total_shards" => 2,
            "scanned_records" => 10,
            "written_entries" => 10,
            "written_bytes" => 900
          },
          "validation" => %{
            "scope" => "catalog_build",
            "status" => "passed",
            "phase_counts" => %{"done" => 2},
            "current_phases" => ["done"],
            "completed_shards" => 2,
            "total_shards" => 2,
            "checked_records" => 10,
            "checked_entries" => 10,
            "mismatches" => 0,
            "failure_reason" => nil,
            "validated_at_ms" => 999_000
          },
          "retirement" => %{"status" => "not_applicable"},
          "statistics" => %{
            "status" => "fresh",
            "samples" => 2,
            "fresh_samples" => 2,
            "stale_samples" => 0,
            "future_samples" => 0,
            "oldest_collected_at_ms" => 998_000,
            "newest_collected_at_ms" => 999_000,
            "oldest_age_ms" => 2_000,
            "newest_age_ms" => 1_000
          }
        }
      ]
    }
  end
end
