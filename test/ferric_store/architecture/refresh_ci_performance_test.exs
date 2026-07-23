defmodule FerricStore.Architecture.RefreshCiPerformanceTest do
  use FerricStore.Test.ArchitectureCase

  test "topology refresh callers own a finite default deadline" do
    requests = source("../../lib/ferric_store/sdk/native/topology_refresh_requests.ex")

    refute requests =~ ":infinity"
    assert requests =~ "{:refresh_topology, context}"
    assert requests =~ "RequestContext.remaining"

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_requests.ex") <=
             40

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_call.ex") <= 65
  end

  test "native pipeline validation is bounded and outside request and protocol facades", %{
    calls: calls
  } do
    source = source("../../lib/ferric_store/sdk/native/pipeline_requests.ex")

    assert source =~ "@pipeline_option_keys [:request_context, :return]"
    refute source("../../lib/ferric_store/protocol.ex") =~ "List.wrap(command)"

    for {caller, callee, function} <- [
          {FerricStore.SDK.Native.PipelineRequests, FerricStore.SDK.Native.PipelineAdmission,
           :admit},
          {FerricStore.SDK.Native.PipelineAdmission, FerricStore.Protocol.PipelineCommand,
           :validate},
          {FerricStore.Protocol.PipelinePayload, FerricStore.Protocol.PipelineCommand, :normalize}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and call.callee_module == callee and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/pipeline_requests.ex") <= 125
    assert source_line_count("../../lib/ferric_store/sdk/native/pipeline_admission.ex") <= 60
    assert source_line_count("../../lib/ferric_store/protocol/pipeline_command.ex") <= 140
    assert source_line_count("../../lib/ferric_store/protocol/pipeline_raw_command.ex") <= 45
    assert source_line_count("../../lib/ferric_store/protocol.ex") <= 160

    for caller <- [
          FerricStore.Protocol.CommandPayload,
          FerricStore.SDK.Native.ClientRequestAdmission
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.Protocol.CommandName and
                 call.callee_function == :normalize
             end)
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.PipelineCommand and
               call.callee_module == FerricStore.Protocol.PipelineRawCommand and
               call.callee_function == :fields
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.PipelineRawCommand and
               call.callee_module == FerricStore.Protocol.CommandName and
               call.callee_function == :normalize
           end)

    assert source_line_count("../../lib/ferric_store/protocol/command_name.ex") <= 35
  end

  test "topology refresh completion scheduling is outside the coordinator", %{calls: calls} do
    callers = %{
      enqueue: FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime,
      resume: FerricStore.SDK.Native.CoordinatorInfoRuntime,
      cancel: FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime
    }

    Enum.each(callers, fn {function, caller} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.TopologyRefreshCompletions and
                 call.callee_function == function
             end)
    end)

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator.ex") <= 2_200

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_completions.ex") <=
             100

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorInfoRuntime and
               call.callee_module == FerricStore.SDK.Native.CoordinatorConnectionCleanup and
               call.callee_function == :discard_refresh
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_cleanup.ex"
           ) <=
             40
  end

  test "topology refresh state transitions are outside the coordinator", %{calls: calls} do
    owners = %{
      initialize: FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
      start: FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
      finish: FerricStore.SDK.Native.CoordinatorInfoRuntime,
      finish_waiter: FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
      cancel: FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
      operation: FerricStore.SDK.Native.CoordinatorInfoRuntime
    }

    for {function, owner} <- owners do
      assert Enum.any?(calls, fn call ->
               call.caller_module == owner and
                 call.callee_module == FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime and
                 call.callee_function == function
             end),
             "#{inspect(owner)} must delegate topology refresh #{function}/N"
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_runtime.ex") <= 160

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_topology_refresh_runtime.ex"
           ) <= 180

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime and
               call.callee_module == FerricStore.SDK.Native.TopologyRefreshWaiter and
               call.callee_function == :finish
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_waiter.ex") <= 60
  end

  test "native connections contain no obsolete deferred GOAWAY stop path" do
    refute source_contains?(
             "../../lib/ferric_store/sdk/native/connection.ex",
             ":stop_after_goaway"
           )
  end

  test "public protocol helpers do not expose trusted batch cardinality envelopes" do
    refute source_contains?("../../lib/ferric_store/protocol.ex", "def custom_batch_payload")
  end

  test "topology unit tests do not use wall-clock performance ratios" do
    topology_test = source("../../test/ferric_store/sdk/native/topology_test.exs")

    refute topology_test =~ ":timer.tc"
    refute topology_test =~ "monotonic_time"
  end

  test "CI and release validate against the immutable released server contract" do
    release_image =
      "ghcr.io/ferricstore/ferricstore:0.10.1@sha256:198cffba8e2df2f5f66db9e6bbef83131f4841d4b90c65ee8091ac463ec6715d"

    refute File.exists?(Path.expand("../../../scripts/server_build_compat.patch", __DIR__))

    for workflow <- [
          "../../../.github/workflows/ci.yml",
          "../../../.github/workflows/release.yml"
        ] do
      source = File.read!(Path.expand(workflow, __DIR__))

      assert source =~ "FERRICSTORE_TEST_IMAGE: \"#{release_image}\""
      assert source =~ ~s|"$FERRICSTORE_TEST_IMAGE"|
      refute source =~ "repository: ferricstore/ferricstore"
      refute source =~ "scripts/build_integration_server.sh"
    end

    integration_script =
      File.read!(Path.expand("../../../scripts/test_integration.sh", __DIR__))

    assert integration_script =~ release_image
    refute integration_script =~ "build_integration_server.sh"
    assert integration_script =~ "mise exec -- mix run"
    assert integration_script =~ "mise exec -- mix test"

    build_script =
      File.read!(Path.expand("../../../scripts/build_integration_server.sh", __DIR__))

    assert build_script =~ "git -C \"$SERVER_SOURCE\" rev-parse HEAD"
    refute build_script =~ "git apply"
    refute build_script =~ "SERVER_PATCH"
  end

  test "connection capacity resumes only explicitly indexed waiting batches" do
    refute function_exported?(FerricStore.SDK.Native.ConnectionPool, :attempt_keys, 1)

    refute source_contains?(
             "../../lib/ferric_store/sdk/native/coordinator.ex",
             "Map.keys(state.pending_batches)"
           )

    refute source_contains?(
             "../../lib/ferric_store/sdk/native/coordinator.ex",
             "ConnectionPool.attempt_keys(state.connection_pool)"
           )
  end

  test "batch routing carries its index without materializing an indexed copy" do
    refute source_contains?(
             "../../lib/ferric_store/sdk/native/batch_preparer.ex",
             "Enum.with_index"
           )
  end

  test "the SDK hot-path benchmark exercises trusted KV preparation" do
    benchmark = File.read!(Path.expand("../../../bench/sdk_hot_path_benchmark.exs", __DIR__))

    assert benchmark =~ "{:topology,"
    assert benchmark =~ ":kv_preparation_admission"
    assert benchmark =~ ":prepared_command_items"
    assert benchmark =~ ":submission_admission"
    assert benchmark =~ ":admitted_submission"
    assert benchmark =~ "{:client, self()}"
    assert benchmark =~ "@shard_count 16"
    assert benchmark =~ "|> Enum.reverse()"
    assert benchmark =~ "operation: :mset"
    assert benchmark =~ "{benchmark-mset}:key-"
    assert benchmark =~ "PreparedMap.metadata"
    assert benchmark =~ ~s(enforce_budget("mset_prepare")

    for workflow <- [
          "../../../.github/workflows/ci.yml",
          "../../../.github/workflows/release.yml"
        ] do
      workflow = workflow |> Path.expand(__DIR__) |> File.read!()

      assert workflow =~ "--max-mget-reductions 70000"
      assert workflow =~ "--max-mset-reductions 250000"
    end

    refute benchmark =~ "{:command_items,"
    refute benchmark =~ ":mget, _items, item_count"
    refute benchmark =~ "atomicity: :per_slot"
    refute benchmark =~ "atomicity: :per_shard"
  end

  test "CI exercises acknowledged connection responses through the KV benchmark" do
    benchmark = File.read!(Path.expand("../../../bench/kv_benchmark.exs", __DIR__))
    benchmark_docs = File.read!(Path.expand("../../../docs/benchmark.md", __DIR__))
    testing_docs = File.read!(Path.expand("../../../docs/testing.md", __DIR__))

    assert benchmark =~ "connect_timeout: 30_000"
    assert benchmark =~ "topology_refresh_timeout: 30_000"
    assert benchmark =~ "min_throughput"
    refute Regex.match?(~r/connect!\([^\n]*\btimeout:/, benchmark)

    for workflow <- [
          "../../../.github/workflows/ci.yml",
          "../../../.github/workflows/release.yml"
        ] do
      workflow = workflow |> Path.expand(__DIR__) |> File.read!()

      assert workflow =~ "mix run bench/kv_benchmark.exs"
      assert workflow =~ "--batch 1"
      assert workflow =~ "--min-throughput 100.0"
    end

    assert benchmark_docs =~ "--batch 1"
    assert benchmark_docs =~ "--min-throughput 100.0"
    assert testing_docs =~ "acknowledged response benchmark"
  end
end
