defmodule FerricStore.Architecture.NativeLifecycleTest do
  use FerricStore.Test.ArchitectureCase

  test "topology coordinator does not spawn a process per request", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator],
      to: [Task],
      functions: [:start, :start_link, :async, :async_stream]
    )
  end

  test "batch callbacks and session bootstrap live outside the coordinator", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.BatchCoordinator and
               call.callee_module == FerricStore.SDK.Native.BatchPreparationStarter and
               call.callee_function == :start
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.BatchPreparationStarter and
               call.callee_module == FerricStore.SDK.Native.BatchPreparer and
               call.callee_function == :start
           end)

    for caller <- [
          FerricStore.SDK.Native.TopologyBootstrap,
          FerricStore.SDK.Native.ConnectionStarter,
          FerricStore.SDK.Native.TopologyRefreshConnection
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.SessionBootstrap and
                 call.callee_function == :establish
             end),
             "#{inspect(caller)} must use the complete shared native session bootstrap"
    end

    for function <- [:bootstrap, :load] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.TopologyRefresher and
                 call.callee_module == FerricStore.SDK.Native.TopologyRefreshConnection and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresher.ex") <= 180

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.TopologyRefresher and
               call.callee_module == FerricStore.SDK.Native.TopologyRefreshCandidates and
               call.callee_function == :run
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_candidates.ex") <=
             40

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.TopologyRefresher and
               call.callee_module == FerricStore.SDK.Native.TopologyReplacementDrain and
               call.callee_function == :await
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_replacement_drain.ex") <=
             35

    assert source_line_count("../../lib/ferric_store/sdk/native/topology_refresh_connection.ex") <=
             70

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.EndpointPolicy and
               call.callee_module == FerricStore.SDK.Native.EndpointValidator
           end),
           "endpoint callback policy must live outside the topology coordinator"

    for function <- [:seed_endpoint?, :host?] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.EndpointPolicy and
                 call.callee_module == FerricStore.SDK.Native.EndpointTrust and
                 call.callee_function == function
             end)
    end

    state_source = source("../../lib/ferric_store/sdk/native/coordinator/state.ex")
    assert state_source =~ ":endpoint_trust"
    refute state_source =~ ":trusted_hosts"

    assert source_line_count("../../lib/ferric_store/sdk/native/endpoint_policy.ex") <= 170
    assert source_line_count("../../lib/ferric_store/sdk/native/endpoint_trust.ex") <= 70
  end

  test "connection and initial-topology lifecycles live outside the coordinator", %{calls: calls} do
    lifecycle_boundaries = [
      {
        FerricStore.SDK.Native.CoordinatorConnectionRuntime,
        FerricStore.SDK.Native.ConnectionLifecycle,
        :track
      },
      {
        FerricStore.SDK.Native.CoordinatorConnectionOrchestrator,
        FerricStore.SDK.Native.ConnectionLifecycle,
        :down
      },
      {
        FerricStore.SDK.Native.CoordinatorConnectionOrchestrator,
        FerricStore.SDK.Native.ConnectionLifecycle,
        :retire
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchCancellation,
        FerricStore.SDK.Native.ConnectionLifecycle,
        :stop_attempts
      },
      {
        FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime,
        FerricStore.SDK.Native.TopologyInitialization,
        :run
      }
    ]

    Enum.each(lifecycle_boundaries, fn {caller, module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == module and call.callee_function == function
             end),
             "#{inspect(caller)} must delegate #{function} to #{inspect(module)}"
    end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.TopologyRuntime and
               call.callee_module == FerricStore.SDK.Native.ConnectionLifecycle and
               call.callee_function == :prune
           end),
           "topology runtime must delegate connection pruning to ConnectionLifecycle"

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.CoordinatorShutdown],
      to: [FerricStore.SDK.Native.ConnectionLifecycle],
      functions: [:stop_all, :stop_supervisor]
    )

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.TopologyBootstrap and
               call.callee_module == FerricStore.SDK.Native.SessionBootstrap
           end)

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator],
      to: [FerricStore.SDK.Native.SessionBootstrap]
    )

    for caller <- [
          FerricStore.SDK.Native.ConnectionStarter,
          FerricStore.SDK.Native.TopologyRefresher
        ],
        function <- [:start, :stop] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.ConnectionLifecycle and
                 call.callee_function == function
             end),
             "#{inspect(caller)} must use ConnectionLifecycle.#{function}/2"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime and
               call.callee_module == FerricStore.SDK.Native.TopologyRefreshStarter and
               call.callee_function == :start
           end),
           "refresh worker setup and capacity accounting must live outside the coordinator"

    assert_no_calls(calls,
      from: [
        FerricStore.SDK.Native.ConnectionStarter,
        FerricStore.SDK.Native.TopologyRefresher
      ],
      to: [DynamicSupervisor],
      functions: [:start_child, :terminate_child]
    )

    source = source("../../lib/ferric_store/sdk/native/coordinator.ex")
    refute String.contains?(source, "DynamicSupervisor.terminate_child")
    refute Regex.match?(~r/\{Connection,\s*endpoint\}/, source)
    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator.ex") <= 2_300
  end

  test "process monitor dispatch has a focused lifecycle owner", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorInfoRuntime and
               call.callee_module == FerricStore.SDK.Native.CoordinatorLifecycleRuntime and
               call.callee_function == :down
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_info_runtime.ex") <=
             175

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorInfoRuntime and
               call.callee_module ==
                 FerricStore.SDK.Native.CoordinatorConnectionStartCompletion and
               call.callee_function == :handle
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_start_completion.ex"
           ) <= 50

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorInfoRuntime and
               call.callee_module == FerricStore.SDK.Native.CoordinatorPendingRequestTimeout and
               call.callee_function == :handle
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_pending_request_timeout.ex"
           ) <= 45

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorRequestOrchestration and
               call.callee_module == FerricStore.SDK.Native.CoordinatorSubmissionRuntime and
               call.callee_function == :routed
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_submission_runtime.ex"
           ) <= 75

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorInfoRuntime and
               call.callee_module == FerricStore.SDK.Native.CoordinatorBatchPreparationRuntime and
               call.callee_function == :complete
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_batch_preparation_runtime.ex"
           ) <= 35

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_batch_orchestrator.ex"
           ) <= 200

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_orchestrator.ex"
           ) <= 180

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_call_runtime.ex") <=
             180

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_retry_runtime.ex") <=
             165

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorRetryRuntime and
               call.callee_module == FerricStore.SDK.Native.CoordinatorRetryTarget and
               call.callee_function == :control
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_retry_target.ex") <=
             25

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_lifecycle_runtime.ex") <=
             100
  end

  test "native endpoint topology policy lives outside the coordinator", %{calls: calls} do
    for function <- [:current, :control_endpoint] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.CoordinatorSubmissionRuntime and
                 call.callee_module == FerricStore.SDK.Native.TopologyRuntime and
                 call.callee_function == function
             end),
             "submission runtime must delegate #{function} to TopologyRuntime"
    end

    for function <- [:put, :candidates, :prune] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.CoordinatorTopologyRefreshRuntime and
                 call.callee_module == FerricStore.SDK.Native.TopologyRuntime and
                 call.callee_function == function
             end),
             "topology refresh runtime must delegate #{function} to TopologyRuntime"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorConnectionAcquisition and
               call.callee_module == FerricStore.SDK.Native.TopologyRuntime and
               call.callee_function == :endpoint_defaults
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorConnectionAcquisition and
               call.callee_module == FerricStore.SDK.Native.CoordinatorConnectionAttempt and
               call.callee_function == :start
           end)

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_attempt.ex"
           ) <= 75

    source = source("../../lib/ferric_store/sdk/native/client_supervisor.ex")
    assert source =~ ":protected"
    refute source =~ "[:set, :public"

    assert source_line_count("../../lib/ferric_store/sdk/native/client_supervisor.ex") <= 90
    assert source_line_count("../../lib/ferric_store/sdk/native/client_runtime_starter.ex") <= 175
    assert source_line_count("../../lib/ferric_store/sdk/native/client_endpoint.ex") <= 145

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientSupervisor and
               call.callee_module == FerricStore.SDK.Native.ClientRuntimeStarter and
               call.callee_function == :start
           end)

    for function <- [:coordinator, :topology_snapshot, :submission_admission, :event_source] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.ClientSupervisor and
                 call.callee_module == FerricStore.SDK.Native.ClientEndpoint and
                 call.callee_function == function
             end)
    end

    endpoint_source = source("../../lib/ferric_store/sdk/native/client_endpoint.ex")
    assert endpoint_source =~ ":ets.give_away"

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator.ex") <= 2_250
  end

  test "topology snapshot construction delegates endpoint identity policy", %{calls: calls} do
    boundaries = [
      {FerricStore.SDK.Native.Topology, :prepare},
      {FerricStore.SDK.Native.Topology, :key},
      {FerricStore.SDK.Native.Topology.EndpointResolver, :key},
      {FerricStore.SDK.Native.Topology.EndpointResolver, :inherited_options},
      {FerricStore.SDK.Native.Topology.EndpointResolver, :normalize_dns_result}
    ]

    for {caller, function} <- boundaries do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.EndpointIdentity and
                 call.callee_function == function
             end),
             "#{inspect(caller)} must delegate #{function} to EndpointIdentity"
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/topology.ex") <= 120
    assert source_line_count("../../lib/ferric_store/sdk/native/topology/builder.ex") <= 200

    assert source_line_count("../../lib/ferric_store/sdk/native/topology/endpoint_resolver.ex") <=
             100

    assert source_line_count("../../lib/ferric_store/sdk/native/endpoint_identity.ex") <= 130
  end
end
