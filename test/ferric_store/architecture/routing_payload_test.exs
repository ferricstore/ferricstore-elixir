defmodule FerricStore.Architecture.RoutingPayloadTest do
  use FerricStore.Test.ArchitectureCase

  test "Flow payload construction is split by command responsibility", %{calls: calls} do
    boundaries = [
      {FerricStore.Flow.Payload.Query, :get_payload},
      {FerricStore.Flow.Payload.Mutation, :create_payload},
      {FerricStore.Flow.Payload.Batch, :create_many_with_count}
    ]

    Enum.each(boundaries, fn {module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Flow.Payload and
                 call.callee_module == module and call.callee_function == function
             end),
             "Flow payload construction must delegate #{function} to #{inspect(module)}"
    end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Payload.Batch and
               call.callee_module == FerricStore.Flow.Payload.CreateManyItems and
               call.callee_function == :map
           end)

    assert source_line_count("../../lib/ferric_store/flow/payload.ex") <= 80

    for relative_path <- [
          "../../lib/ferric_store/flow/payload/query.ex",
          "../../lib/ferric_store/flow/payload/mutation.ex",
          "../../lib/ferric_store/flow/payload/batch.ex",
          "../../lib/ferric_store/flow/payload/policy.ex",
          "../../lib/ferric_store/flow/payload/normalize.ex"
        ] do
      assert source_line_count(relative_path) <= 240
    end

    for caller <- [
          FerricStore.Flow.ValueCommands,
          FerricStore.Flow.Payload.CreateManyItems,
          FerricStore.Flow.Payload.Normalize
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.Flow.CodecRuntime and
                 call.callee_function == :encode
             end)
    end

    assert source_line_count("../../lib/ferric_store/flow/codec_runtime.ex") <= 55
    assert source_line_count("../../lib/ferric_store/flow/payload/create_many_items.ex") <= 80
    assert source_line_count("../../lib/ferric_store/flow/payload/create_many_map_item.ex") <= 80
    assert source_line_count("../../lib/ferric_store/flow/codec_error.ex") <= 10

    for function <- [:set_payload, :get_payload] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Flow and
                 call.callee_module == FerricStore.Flow.PolicyCommand and
                 call.callee_function == function
             end)
    end

    refute function_exported?(FerricStore.Flow.Payload, :policy_set_payload, 2)
    refute function_exported?(FerricStore.Flow.Payload.Policy, :policy_set_payload, 2)

    for relative_path <- [
          "../../lib/ferric_store/flow/policy_command.ex",
          "../../lib/ferric_store/flow/policy_normalizer.ex",
          "../../lib/ferric_store/flow/policy_state_validator.ex",
          "../../lib/ferric_store/flow/policy_validation.ex",
          "../../lib/ferric_store/flow/policy_value_validator.ex"
        ] do
      assert source_line_count(relative_path) <= 60
    end

    assert source_line_count("../../lib/ferric_store/flow/policy_structure.ex") <= 160
    assert source_line_count("../../lib/ferric_store/flow/policy_index_validator.ex") <= 100
    assert source_line_count("../../lib/ferric_store/flow/policy_retry_validator.ex") <= 100

    for {caller, callee, function} <- [
          {FerricStore.Flow.PolicyStructure, FerricStore.Flow.PolicyOptionStructure, :option_map},
          {FerricStore.Flow.PolicyStructure, FerricStore.Flow.PolicyStateStructure, :validate},
          {FerricStore.Flow.PolicyNormalizer, FerricStore.Flow.PolicyValueNormalizer, :normalize},
          {FerricStore.Flow.PolicyCommand, FerricStore.Flow.PolicyStateSelector, :validate}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and call.callee_module == callee and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/flow/policy_option_structure.ex") <= 80
    assert source_line_count("../../lib/ferric_store/flow/policy_state_structure.ex") <= 125
    assert source_line_count("../../lib/ferric_store/flow/policy_value_normalizer.ex") <= 40
    assert source_line_count("../../lib/ferric_store/flow/policy_state_selector.ex") <= 25
  end

  test "public routing and result policy have single owners", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Client and
               call.callee_module == FerricStore.RouteKey and
               call.callee_function == :from_options
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Flow and
               call.callee_module == FerricStore.FlowRouting and
               call.callee_function == :resolve_payload
           end)

    for caller <- [FerricStore, FerricStore.Client] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.Result and
                 call.callee_function == :unwrap
             end)
    end
  end

  test "native request routing uses the canonical route-key validator", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.RouteKey and
               call.callee_function == :validate
           end)

    refute source("../../lib/ferric_store/sdk/native/client_request_admission.ex") =~
             "def validate_route_key"
  end

  test "coordinator cleanup never waits synchronously for a connection", %{calls: calls} do
    cleanup_modules = [
      FerricStore.SDK.Native.CoordinatorInfoRuntime,
      FerricStore.SDK.Native.BatchRequestCancellation,
      FerricStore.SDK.Native.CoordinatorEventCancellation
    ]

    assert_no_calls(calls,
      from: cleanup_modules,
      to: [FerricStore.SDK.Native.Connection],
      functions: [:cancel]
    )

    Enum.each(cleanup_modules, fn module ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == module and
                 call.callee_module == FerricStore.SDK.Native.Connection and
                 call.callee_function == :cancel_async
             end)
    end)
  end

  test "event dispatch queue policy is isolated from worker lifecycle", %{calls: calls} do
    for function <- [:commit, :cancel, :drop_uncommitted] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Transport.EventDispatcher and
                 call.callee_module == FerricStore.Transport.EventDispatcherQueue and
                 call.callee_function == function
             end)
    end

    for function <- [:initialize, :take_committed] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Transport.EventDispatcherExecution and
                 call.callee_module == FerricStore.Transport.EventDispatcherQueue and
                 call.callee_function == function
             end)
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Transport.EventDispatcher and
               call.callee_module == FerricStore.Transport.EventDispatcherExecution and
               call.callee_function == :initialize
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Transport.EventDispatcherAdmission and
               call.callee_module == FerricStore.Transport.EventDispatcherQueue and
               call.callee_function == :prepare
           end)

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher.ex") <= 200

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_execution.ex") <=
             90

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_admission.ex") <=
             50

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_stats.ex") <=
             35

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_options.ex") <=
             30

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_shutdown.ex") <=
             25

    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher_queue.ex") <=
             180
  end

  test "flow routing rejects ambiguous payload fields through RouteKey", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.FlowRouting and
               call.callee_module == FerricStore.RouteKey and
               call.callee_function == :ensure_unambiguous_payload_fields
           end)

    assert source_line_count("../../lib/ferric_store/route_key.ex") <= 110

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.FlowRouting and
               call.callee_module == FerricStore.FlowRouting.PartitionList and
               call.callee_function == :resolve
           end)

    assert source_line_count("../../lib/ferric_store/flow_routing.ex") <= 210
    assert source_line_count("../../lib/ferric_store/flow_routing/partition_list.ex") <= 80
    assert source_line_count("../../lib/ferric_store/flow_routing/route_source.ex") <= 80
  end

  test "public KV facade contains no legacy option aliases" do
    source = source("../../lib/ferric_store.ex")
    Code.ensure_loaded!(FerricStore.SDK.Native.ClientRequests)
    Code.ensure_loaded!(FerricStore.SDK.KV.Input)
    Code.ensure_loaded!(FerricStore.SDK.KV)

    refute source =~ ":ttl_ms"
    refute source =~ ":with_scores"

    refute function_exported?(
             FerricStore.SDK.Native.ClientRequests,
             :request_by_items_with_known_count,
             7
           )

    refute function_exported?(FerricStore.SDK.KV.Input, :mset_route_key, 1)
    refute function_exported?(FerricStore.SDK.KV.Input, :mset_group_payload, 1)
    refute function_exported?(FerricStore.SDK.KV, :fetch_or_compute_result, 4)
    refute function_exported?(FerricStore.SDK.KV, :fetch_or_compute_error, 3)

    refute function_exported?(FerricStore.SDK.Native.Client, :get, 3)
    refute function_exported?(FerricStore.SDK.Native.Client, :set, 4)
    refute function_exported?(FerricStore.SDK.Native.ClientRequests, :get, 3)
    refute function_exported?(FerricStore.SDK.Native.ClientRequests, :set, 4)
  end

  test "Flow exposes one typed payload contract with no command grammar", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Flow, FerricStore.SDK.Flow],
      to: [FerricStore.Client, FerricStore.SDK.Native.Client],
      functions: [:command, :command_exec]
    )

    refute Code.ensure_loaded?(FerricStore.Flow.CommandOptions)
    refute function_exported?(FerricStore.FlowRouting, :resolve_command, 3)

    for function <- [
          :create_args,
          :claim_due_args,
          :transition_args,
          :complete_args,
          :retry_args,
          :fail_args,
          :cancel_args,
          :policy_set_args
        ] do
      refute function_exported?(FerricStore.Flow, function, 2),
             "legacy Flow argument builder #{function}/2 must not be exported"
    end
  end
end
