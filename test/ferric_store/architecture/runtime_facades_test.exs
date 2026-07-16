defmodule FerricStore.Architecture.RuntimeFacadesTest do
  use FerricStore.Test.ArchitectureCase

  test "coordinator callback constants do not compile-capture orchestration modules" do
    source = source("../../lib/ferric_store/sdk/native/coordinator_runtime_callbacks.ex")

    refute Regex.match?(
             ~r/:\s*&Coordinator(?:Batch|Lifecycle|Request)Orchestration\./,
             source
           )
  end

  test "coordinator orchestration does not depend back on its callback facade", %{calls: calls} do
    assert_no_calls(calls,
      from: [
        FerricStore.SDK.Native.CoordinatorBatchOrchestration,
        FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
        FerricStore.SDK.Native.CoordinatorRequestOrchestration
      ],
      to: [FerricStore.SDK.Native.CoordinatorRuntimeCallbacks]
    )
  end

  test "large runtime facades delegate focused responsibilities", %{calls: calls} do
    boundaries = [
      {FerricStore.Flow, FerricStore.Flow.Payload, :create_payload},
      {FerricStore.Flow.LifecycleCommands, FerricStore.Flow.CommandRuntime, :with_options},
      {FerricStore.Flow, FerricStore.Flow.ValueCommands, :put},
      {FerricStore.Flow, FerricStore.Flow.ValueCommands, :mget},
      {FerricStore.Protocol.FrameCodec, FerricStore.Protocol.ResponseDecoder, :decode},
      {FerricStore.SDK.KV, FerricStore.SDK.KV.CollectionCommands, :del},
      {
        FerricStore.SDK.Native.ServerContract,
        FerricStore.SDK.Native.ServerContractShape,
        :validate
      },
      {
        FerricStore.SDK.Native.ServerContract,
        FerricStore.SDK.Native.ServerSessionContract,
        :validate
      },
      {
        FerricStore.SDK.Native.Connection,
        FerricStore.SDK.Native.ConnectionRequest,
        :submit
      },
      {
        FerricStore.SDK.Native.ConnectionClient,
        FerricStore.SDK.Native.ConnectionTimers,
        :request_deadline
      },
      {
        FerricStore.SDK.Native.ConnectionEncodingWorker,
        FerricStore.SDK.Native.PipelinePreparer,
        :prepare
      },
      {
        FerricStore.SDK.Native.ConnectionEncoder,
        FerricStore.SDK.Native.ConnectionEncodingWorker,
        :start
      },
      {
        FerricStore.SDK.Native.ClientRequests,
        FerricStore.SDK.Native.ClientRequestAdmission,
        :prepare_context
      },
      {
        FerricStore.Flow.BatchCommands,
        FerricStore.Flow.BatchRuntime,
        :request
      },
      {
        FerricStore.Flow.BatchRuntime,
        FerricStore.SDK.Native.PreparedRequests,
        :request_trusted_batch
      },
      {
        FerricStore.SDK.Native.PreparedRequests,
        FerricStore.RequestContext,
        :with_batch_item_count
      },
      {
        FerricStore.Client,
        FerricStore.SDK.Native.Client,
        :request_trusted_batch
      },
      {
        FerricStore.SDK.Native.Client,
        FerricStore.SDK.Native.ClientBatchRequests,
        :request_trusted
      },
      {
        FerricStore.SDK.Native.ClientBatchRequests,
        FerricStore.RequestContext,
        :with_batch_item_count
      },
      {
        FerricStore.SDK.Native.ClientCommandRequests,
        FerricStore.Protocol.RequestContextCodec,
        :put_result
      },
      {
        FerricStore.SDK.Native.EventSubscriptionAdmission,
        FerricStore.SDK.Native.EventFilterValidator,
        :validate
      },
      {
        FerricStore.SDK.Native.EventSubscriptions,
        FerricStore.SDK.Native.EventSubscriptionWirePolicy,
        :subscribe_wire_events
      },
      {
        FerricStore.SDK.Native.EventSubscriptions,
        FerricStore.SDK.Native.EventSubscriptionWirePolicy,
        :unsubscribe_wire_events
      },
      {
        FerricStore.SDK.Native.BatchPreparer,
        FerricStore.SDK.Native.BatchGroupPreparer,
        :prepare
      },
      {
        FerricStore.SDK.Native.BatchRouter,
        FerricStore.SDK.Native.BatchItemRouter,
        :call
      },
      {
        FerricStore.SDK.Native.BatchGroupPreparer,
        FerricStore.SDK.Native.BatchGroupCallbacks,
        :build_payload
      },
      {
        FerricStore.SDK.Native.BatchGroupPreparer,
        FerricStore.SDK.Native.BatchGroupCallbacks,
        :prepare_group
      },
      {
        FerricStore.SDK.Native.BatchGroupCallbacks,
        FerricStore.SDK.Native.ClientRequestAdmission,
        :validate_external_payload
      },
      {
        FerricStore.SDK.Native.ConnectionRequest,
        FerricStore.SDK.Native.ConnectionPending,
        :fail_all
      },
      {
        FerricStore.SDK.Native.ConnectionRequest,
        FerricStore.SDK.Native.ConnectionPending,
        :register
      },
      {
        FerricStore.SDK.Native.ConnectionPendingRegistration,
        FerricStore.SDK.Native.FlowControl,
        :increment
      },
      {
        FerricStore.SDK.Native.ConnectionServerFrameDecoder,
        FerricStore.SDK.Native.ConnectionEventHandler,
        :deliver
      },
      {
        FerricStore.SDK.Native.ConnectionFrameProcessor,
        FerricStore.SDK.Native.ConnectionServerFrameRuntime,
        :begin
      },
      {
        FerricStore.SDK.Native.ConnectionFrameProcessor,
        FerricStore.Transport.ResponseAssembler,
        :append
      },
      {
        FerricStore.SDK.Native.ConnectionFrameProcessor,
        FerricStore.Transport.ResponseAssembler,
        :complete_parts
      },
      {
        FerricStore.SDK.Native.ConnectionFrameProcessor,
        FerricStore.Transport.ServerFrameAssembler,
        :append
      },
      {
        FerricStore.Transport.ServerFrameAssembler,
        FerricStore.Transport.ServerChunk,
        :new
      },
      {
        FerricStore.SDK.Native.ConnectionInfoRuntime,
        FerricStore.SDK.Native.ConnectionSocketRuntime,
        :data
      },
      {
        FerricStore.SDK.Native.ConnectionInfoRuntime,
        FerricStore.SDK.Native.ConnectionSocketRuntime,
        :continue
      },
      {
        FerricStore.SDK.Native.ConnectionInfoRuntime,
        FerricStore.SDK.Native.ConnectionSocketRuntime,
        :down
      },
      {
        FerricStore.SDK.Native.ConnectionSocketRuntime,
        FerricStore.SDK.Native.ConnectionFrameProcessor,
        :process
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchOrchestrator,
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        :advance
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        FerricStore.SDK.Native.BatchPreflight,
        :start
      },
      {
        FerricStore.SDK.Native.BatchPreflight,
        FerricStore.SDK.Native.BatchPreflightCompletion,
        :finish
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        FerricStore.SDK.Native.BatchExecution,
        :advance
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        FerricStore.SDK.Native.CoordinatorBatchCompletion,
        :finish
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        FerricStore.SDK.Native.CoordinatorBatchCancellation,
        :cancel
      },
      {
        FerricStore.SDK.Native.CoordinatorBatchRuntime,
        FerricStore.SDK.Native.CoordinatorBatchWaiters,
        :resume_endpoint
      },
      {
        FerricStore.SDK.Native.CoordinatorConnectionAcquisition,
        FerricStore.SDK.Native.CoordinatorConnectionRuntime,
        :remove_waiter
      },
      {
        FerricStore.SDK.Native.CoordinatorConnectionAcquisition,
        FerricStore.SDK.Native.CoordinatorConnectionAttempt,
        :start
      },
      {
        FerricStore.SDK.Native.ConnectionPool,
        FerricStore.SDK.Native.ConnectionPoolCheckout,
        :checkout
      },
      {
        FerricStore.SDK.Native.ConnectionPool,
        FerricStore.SDK.Native.ConnectionPoolRegistry,
        :track
      },
      {
        FerricStore.SDK.Native.ConnectionPool,
        FerricStore.SDK.Native.ConnectionPoolRefreshCapacity,
        :reserve
      },
      {
        FerricStore.SDK.Native.ConnectionAttempts,
        FerricStore.SDK.Native.ConnectionAttemptBatchIndex,
        :put
      },
      {
        FerricStore.SDK.Native.ConnectionOptions,
        FerricStore.SDK.Native.ConnectionOptionValidator,
        :valid?
      },
      {
        FerricStore.Protocol.ResponseDecoder,
        FerricStore.Protocol.CompactMGetDecoder,
        :decode
      },
      {
        FerricStore.Protocol.ResponseDecoder,
        FerricStore.Protocol.CompactValueDecoder,
        :decode
      },
      {
        FerricStore.Protocol.ResponseDecoder,
        FerricStore.Protocol.CompactPipelineDecoder,
        :decode
      },
      {
        FerricStore.Protocol.CompactPipelineDecoder,
        FerricStore.Protocol.CompactPipelineItems,
        :decode
      },
      {
        FerricStore.SDK.Native.ConnectionEncodingWorker,
        FerricStore.Protocol.ResponsePlan,
        :build
      },
      {
        FerricStore.Protocol.ResponseDecoder,
        FerricStore.Protocol.CompactClaimDecoder,
        :decode
      },
      {
        FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
        FerricStore.SDK.Native.EventRestoration,
        :reconnect
      },
      {
        FerricStore.Transport.EventDispatcher,
        FerricStore.Transport.EventDispatcherClient,
        :dispatch
      },
      {
        FerricStore.SDK.Native.CoordinatorRuntime,
        FerricStore.SDK.Native.CoordinatorShutdown,
        :run
      },
      {
        FerricStore.SDK.Native.CoordinatorRuntime,
        FerricStore.SDK.Native.CoordinatorInitializer,
        :run
      },
      {
        FerricStore.SDK.Native.CoordinatorRequestRuntime,
        FerricStore.SDK.Native.CoordinatorTimers,
        :cancel
      },
      {
        FerricStore.SDK.Native.Coordinator.State,
        FerricStore.SDK.Native.Coordinator.StateEvents,
        :subscriptions
      },
      {
        FerricStore.SDK.Native.CoordinatorEventOperationRuntime,
        FerricStore.SDK.Native.EventRequest,
        :operation
      },
      {
        FerricStore.SDK.Native.CoordinatorEventOperationRuntime,
        FerricStore.SDK.Native.EventCommit,
        :subscribe
      },
      {
        FerricStore.SDK.Native.CoordinatorLifecycleOrchestration,
        FerricStore.SDK.Native.CoordinatorEventRuntime,
        :enqueue
      },
      {
        FerricStore.SDK.Native.CoordinatorSubmissionRuntime,
        FerricStore.SDK.Native.CoordinatorRequest,
        :control
      },
      {
        FerricStore.SDK.Native.BatchCoordinator,
        FerricStore.SDK.Native.BatchOperation,
        :new
      },
      {
        FerricStore.SDK.Native.CoordinatorCallRuntime,
        FerricStore.SDK.Native.BatchCoordinator,
        :dispatch_items
      },
      {
        FerricStore.SDK.Native.CoordinatorCallRuntime,
        FerricStore.SDK.Native.KVPreparationCoordinator,
        :admit
      },
      {
        FerricStore.SDK.Native.CoordinatorCallRuntime,
        FerricStore.SDK.Native.EventSubscriptionCoordinator,
        :prepare
      },
      {
        FerricStore.SDK.Native.CoordinatorRuntime,
        FerricStore.SDK.Native.CoordinatorCallRuntime,
        :handle
      },
      {
        FerricStore.SDK.Native.Coordinator,
        FerricStore.SDK.Native.CoordinatorInfoRuntime,
        :handle
      },
      {
        FerricStore.SDK.Native.Coordinator,
        FerricStore.SDK.Native.CoordinatorRuntime,
        :call
      }
    ]

    Enum.each(boundaries, fn {caller, module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and call.callee_module == module and
                 call.callee_function == function
             end),
             "#{inspect(caller)} must delegate #{function} to #{inspect(module)}"
    end)

    assert source_line_count("../../lib/ferric_store/flow.ex") <= 100

    for {callee, function} <- [
          {FerricStore.Flow.LifecycleCommands, :create},
          {FerricStore.Flow.QueryCommands, :get},
          {FerricStore.Flow.BatchCommands, :create_many},
          {FerricStore.Flow.PolicyCommands, :set}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Flow and call.callee_module == callee and
                 call.callee_function == function
             end)
    end

    for path <- [
          "../../lib/ferric_store/flow/lifecycle_commands.ex",
          "../../lib/ferric_store/flow/query_commands.ex",
          "../../lib/ferric_store/flow/batch_commands.ex",
          "../../lib/ferric_store/flow/policy_commands.ex"
        ] do
      assert source_line_count(path) <= 125
    end

    assert source_line_count("../../lib/ferric_store/flow/command_runtime.ex") <= 40
    assert source_line_count("../../lib/ferric_store/flow/argument_validator.ex") <= 20
    assert source_line_count("../../lib/ferric_store/flow/value_commands.ex") <= 75
    assert source_line_count("../../lib/ferric_store/flow/value_refs_validator.ex") <= 30
    assert source_line_count("../../lib/ferric_store/flow/batch_runtime.ex") <= 60
    assert source_line_count("../../lib/ferric_store/flow/response.ex") <= 100
    assert source_line_count("../../lib/ferric_store/flow/record_response_decoder.ex") <= 70
    assert source_line_count("../../lib/ferric_store/flow/response_records.ex") <= 60
    assert source_line_count("../../lib/ferric_store/flow/claim_normalizer.ex") <= 80
    assert source_line_count("../../lib/ferric_store/flow/claim_validator.ex") <= 45

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.ClaimNormalizer and
               call.callee_module == FerricStore.Flow.ClaimValidator and
               call.callee_function == :validate
           end)

    assert source_line_count("../../lib/ferric_store/flow/claim_response_decoder.ex") <= 45
    assert source_line_count("../../lib/ferric_store/flow/response_result_list.ex") <= 35
    assert source_line_count("../../lib/ferric_store/flow/options.ex") <= 100
    assert source_line_count("../../lib/ferric_store/flow/options/type_validator.ex") <= 50

    assert source_line_count("../../lib/ferric_store/flow/options/numeric_value_validator.ex") <=
             125

    assert source_line_count("../../lib/ferric_store/flow/options/cross_value_validator.ex") <=
             110

    assert source_line_count("../../lib/ferric_store/flow/options/string_value_validator.ex") <=
             125

    assert source_line_count("../../lib/ferric_store/flow/options/partition_value_validator.ex") <=
             55

    for {path, limit} <- [
          {"../../lib/ferric_store/flow/options/collection_validator.ex", 25},
          {"../../lib/ferric_store/flow/options/collection_scan.ex", 45},
          {"../../lib/ferric_store/flow/options/claim_collection_validator.ex", 85},
          {"../../lib/ferric_store/flow/options/name_collection_validator.ex", 45},
          {"../../lib/ferric_store/flow/options/query_collection_validator.ex", 55}
        ] do
      assert source_line_count(path) <= limit
    end

    assert source_line_count("../../lib/ferric_store/flow/options/required_value_validator.ex") <=
             65

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.ValueValidator and
               call.callee_module == FerricStore.Flow.Options.TypeValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.ValueCommands and
               call.callee_module == FerricStore.Flow.ValueRefsValidator and
               call.callee_function == :validate
           end)

    for caller <- [
          FerricStore.Flow.LifecycleCommands,
          FerricStore.Flow.QueryCommands,
          FerricStore.Flow.PolicyCommands
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.Flow.ArgumentValidator and
                 call.callee_function == :validate
             end)
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.ValueValidator and
               call.callee_module == FerricStore.Flow.Options.RequiredValueValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.ValueValidator and
               call.callee_module == FerricStore.Flow.Options.NumericValueValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.ValueValidator and
               call.callee_module == FerricStore.Flow.Options.CrossValueValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.ValueValidator and
               call.callee_module == FerricStore.Flow.Options.StringValueValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options.StringValueValidator and
               call.callee_module == FerricStore.Flow.Options.PartitionValueValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Flow.Options and
               call.callee_module == FerricStore.Flow.Options.CollectionValidator and
               call.callee_function == :validate
           end)

    for callee <- [
          FerricStore.Flow.Options.ClaimCollectionValidator,
          FerricStore.Flow.Options.NameCollectionValidator,
          FerricStore.Flow.Options.QueryCollectionValidator
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Flow.Options.CollectionValidator and
                 call.callee_module == callee and call.callee_function == :validate
             end)
    end

    for {caller, callee, function} <- [
          {FerricStore.Flow.Response, FerricStore.Flow.ClaimResponseDecoder, :decode_raw},
          {FerricStore.Flow.ClaimResponseDecoder, FerricStore.Flow.ClaimNormalizer, :normalize},
          {FerricStore.Flow.ClaimResponseDecoder, FerricStore.Flow.ResponseRecords,
           :decode_record},
          {FerricStore.Flow.ClaimResponseDecoder, FerricStore.Flow.ResponseResultList, :map}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and call.callee_module == callee and
                 call.callee_function == function
             end)
    end

    refute Code.ensure_loaded?(FerricStore.Flow.ResponseList)

    assert source_line_count("../../lib/ferric_store/flow/options/mutation_schema.ex") <= 180
    assert source_line_count("../../lib/ferric_store/flow/options/query_schema.ex") <= 120
    assert source_line_count("../../lib/ferric_store/sdk/native/connection.ex") <= 230
    assert source_line_count("../../lib/ferric_store/sdk/native/connection_initializer.ex") <= 90

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_info_runtime.ex") <=
             170

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_client.ex") <= 100
    assert source_line_count("../../lib/ferric_store/sdk/native/server_contract.ex") <= 200
    assert source_line_count("../../lib/ferric_store/sdk/native/server_contract_shape.ex") <= 40

    assert source_line_count("../../lib/ferric_store/sdk/native/server_session_contract.ex") <=
             30

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_shutdown.ex") <= 40

    for {module, function} <- [
          {FerricStore.SDK.Native.ConnectionClient, :request},
          {FerricStore.SDK.Native.ConnectionInitializer, :run},
          {FerricStore.SDK.Native.ConnectionInfoRuntime, :handle},
          {FerricStore.SDK.Native.ConnectionShutdown, :run}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.Connection and
                 call.callee_module == module and call.callee_function == function
             end),
             "Connection must delegate #{function} to #{inspect(module)}"
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_socket_runtime.ex") <=
             85

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_frame_processor.ex") <=
             210

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_drain.ex") <= 50
    assert source_line_count("../../lib/ferric_store/sdk/native/connection_request.ex") <= 170
    assert source_line_count("../../lib/ferric_store/sdk/native/connection_encoder.ex") <= 140
    assert source_line_count("../../lib/ferric_store/sdk/native/connection_pending.ex") <= 25

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/connection_pending_registration.ex"
           ) <= 85

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_pending_lifecycle.ex") <=
             100

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_encoding_worker.ex") <=
             130

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/connection_server_frame_decoder.ex"
           ) <= 100

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/connection_server_frame_runtime.ex"
           ) <= 115

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_pool.ex") <= 220

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/connection_pool_refresh_capacity.ex"
           ) <= 35

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/connection_attempt_batch_index.ex"
           ) <= 40

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_options.ex") <= 160

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_option_validator.ex") <=
             80

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_pool_checkout.ex") <=
             210

    assert source_line_count("../../lib/ferric_store/sdk/native/connection_pool_registry.ex") <=
             190

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator.ex") <= 30

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_info_runtime.ex") <=
             210

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_runtime.ex") <= 160

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_runtime_callbacks.ex") <=
             230

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_request_orchestration.ex"
           ) <= 170

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_batch_orchestration.ex"
           ) <= 130

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_lifecycle_orchestration.ex"
           ) <= 140

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_request_runtime.ex") <=
             200

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_event_runtime.ex") <=
             45

    for {callee, function} <- [
          {FerricStore.SDK.Native.CoordinatorEventQueueRuntime, :enqueue},
          {FerricStore.SDK.Native.CoordinatorEventQueueRuntime, :timeout},
          {FerricStore.SDK.Native.CoordinatorEventCancellation, :abandon},
          {FerricStore.SDK.Native.CoordinatorEventCompletion, :complete_request}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.CoordinatorEventRuntime and
                 call.callee_module == callee and call.callee_function == function
             end)
    end

    for path <- [
          "../../lib/ferric_store/sdk/native/coordinator_event_queue_runtime.ex",
          "../../lib/ferric_store/sdk/native/coordinator_event_cancellation.ex",
          "../../lib/ferric_store/sdk/native/coordinator_event_completion.ex",
          "../../lib/ferric_store/sdk/native/coordinator_event_operation_runtime.ex",
          "../../lib/ferric_store/sdk/native/coordinator_event_connection_runtime.ex"
        ] do
      assert source_line_count(path) <= 125
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_batch_runtime.ex") <=
             180

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_batch_completion.ex") <=
             120

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_batch_cancellation.ex"
           ) <= 80

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator_batch_waiters.ex") <=
             100

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_batch_wire_runtime.ex"
           ) <=
             80

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_runtime.ex"
           ) <=
             130

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/coordinator_connection_acquisition.ex"
           ) <= 201

    assert source_line_count("../../lib/ferric_store/sdk/native/client_request_admission.ex") <=
             100

    assert source_line_count("../../lib/ferric_store/sdk/native/event_subscription_admission.ex") <=
             50

    assert source_line_count("../../lib/ferric_store/sdk/native/event_subscriptions.ex") <= 175

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/event_subscription_wire_policy.ex"
           ) <= 90

    assert source_line_count("../../lib/ferric_store/sdk/native/event_identifier.ex") <= 40

    assert source_line_count("../../lib/ferric_store/sdk/native/event_filter_validator.ex") <= 60

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_group_preparer.ex") <= 70
    assert source_line_count("../../lib/ferric_store/sdk/native/batch_group_callbacks.ex") <= 60

    assert source_line_count("../../lib/ferric_store/protocol/response_decoder.ex") <= 125
    assert source_line_count("../../lib/ferric_store/protocol/compact_value_decoder.ex") <= 210
    assert source_line_count("../../lib/ferric_store/protocol/compact_mget_decoder.ex") <= 60
    assert source_line_count("../../lib/ferric_store/protocol/compact_claim_decoder.ex") <= 85

    assert source_line_count("../../lib/ferric_store/protocol/compact_pipeline_decoder.ex") <=
             30

    assert source_line_count("../../lib/ferric_store/protocol/compact_pipeline_items.ex") <= 180
    assert source_line_count("../../lib/ferric_store/protocol/response_plan.ex") <= 110

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_execution.ex") <= 55

    for {callee, function} <- [
          {FerricStore.SDK.Native.BatchWireExecution, :advance},
          {FerricStore.SDK.Native.BatchPreflightReservations, :record},
          {FerricStore.SDK.Native.BatchRequestCancellation, :cancel},
          {FerricStore.SDK.Native.BatchCompletion, :take}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.BatchExecution and
                 call.callee_module == callee and call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_wire_execution.ex") <= 175

    for path <- [
          "../../lib/ferric_store/sdk/native/batch_preflight_reservations.ex",
          "../../lib/ferric_store/sdk/native/batch_request_cancellation.ex",
          "../../lib/ferric_store/sdk/native/batch_completion.ex"
        ] do
      assert source_line_count(path) <= 85
    end

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_connection_queue.ex") <=
             155

    for {callee, function} <- [
          {FerricStore.SDK.Native.BatchConnectionQueueEndpoint, :take},
          {FerricStore.SDK.Native.BatchConnectionQueueOrder, :pop}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.BatchConnectionQueue and
                 call.callee_module == callee and call.callee_function == function
             end)
    end

    assert source_line_count(
             "../../lib/ferric_store/sdk/native/batch_connection_queue_endpoint.ex"
           ) <= 135

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_connection_queue_order.ex") <=
             70

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_preflight.ex") <= 190

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_preflight_completion.ex") <=
             35

    assert source_line_count("../../lib/ferric_store/sdk/native/event_restoration.ex") <= 190
    assert source_line_count("../../lib/ferric_store/transport/event_dispatcher.ex") <= 380
    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator/state.ex") <= 340

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_coordinator.ex") <= 170

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_preparation_starter.ex") <=
             100

    for relative_path <- [
          "../../lib/ferric_store/sdk/native/event_call.ex",
          "../../lib/ferric_store/sdk/native/event_request.ex",
          "../../lib/ferric_store/sdk/native/coordinator_request.ex",
          "../../lib/ferric_store/sdk/native/batch_operation.ex"
        ] do
      assert source_line_count(relative_path) <= 160
    end
  end
end
