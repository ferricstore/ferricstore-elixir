defmodule FerricStore.Architecture.CoordinatorStateTest do
  use FerricStore.Test.ArchitectureCase

  test "coordinator state-machine policies live behind focused modules", %{calls: calls} do
    boundaries = [
      {
        FerricStore.SDK.Native.CoordinatorSubmissionRuntime,
        FerricStore.SDK.Native.Admission,
        :full?
      },
      {FerricStore.SDK.Native.BatchCompletion, FerricStore.SDK.Native.BatchPolicy, :completion},
      {
        FerricStore.SDK.Native.CoordinatorBatchPreparationRuntime,
        FerricStore.SDK.Native.BatchScheduler,
        :put
      },
      {
        FerricStore.SDK.Native.ClientRequestAdmission,
        FerricStore.SDK.Native.ClientOptions,
        :validate
      },
      {
        FerricStore.SDK.Native.ClientOptions,
        FerricStore.SDK.Native.ClientSeedOptions,
        :validate
      },
      {
        FerricStore.SDK.Native.ClientLifecycleRequests,
        FerricStore.SDK.Native.EventSubscriptionAdmission,
        :prepare
      },
      {
        FerricStore.SDK.Native.CoordinatorConnectionAcquisition,
        FerricStore.SDK.Native.ConnectionPool,
        :checkout
      },
      {
        FerricStore.SDK.Native.CoordinatorEventQueueRuntime,
        FerricStore.SDK.Native.EventCoordinator,
        :enqueue
      },
      {
        FerricStore.SDK.Native.CoordinatorEventOperationRuntime,
        FerricStore.SDK.Native.EventCall,
        :plan
      },
      {
        FerricStore.SDK.Native.CoordinatorServerEventRuntime,
        FerricStore.SDK.Native.EventFanout,
        :dispatch
      },
      {
        FerricStore.SDK.Native.Coordinator.State,
        FerricStore.SDK.Native.Coordinator.PendingRequests,
        :put
      },
      {
        FerricStore.SDK.Native.CoordinatorCallRuntime,
        FerricStore.SDK.Native.TopologyManager,
        :route
      }
    ]

    Enum.each(boundaries, fn {caller, module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == module and call.callee_function == function
             end),
             "#{inspect(caller)} must delegate #{function} to #{inspect(module)}"
    end)

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator],
      to: [FerricStore.SDK.Native.EventRouter],
      functions: [:deliver]
    )

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.EventRouter],
      to: [Map],
      functions: [:keys]
    )

    refute source("../../lib/ferric_store/sdk/native/event_router.ex") =~ "Map.keys"

    refute Code.ensure_loaded?(FerricStore.SDK.Native.EventState)
    refute function_exported?(FerricStore.SDK.Native.EventSubscriptions, :deliver, 4)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.TopologyBootstrap and
               call.callee_module == FerricStore.SDK.Native.EndpointPolicy and
               call.callee_function == :validate
           end),
           "initial topology lifecycle must own endpoint validation"

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.EventFanout and
               call.callee_module == FerricStore.SDK.Native.EventSubscriptions and
               call.callee_function == :event_kind
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.EventFanout and
               call.callee_module == FerricStore.SDK.Native.EventRouter and
               call.callee_function == :deliver
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.EventCoordinator and
               call.callee_module == FerricStore.SDK.Native.EventQueue and
               call.callee_function == :enqueue
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.EventCoordinator and
               call.callee_module == FerricStore.SDK.Native.EventSubscriberReservations and
               call.callee_function == :reserve
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/event_coordinator.ex") <= 180

    assert source_line_count("../../lib/ferric_store/sdk/native/event_subscriber_reservations.ex") <=
             75

    assert source_line_count("../../lib/ferric_store/sdk/native/client_options.ex") <= 190
    assert source_line_count("../../lib/ferric_store/sdk/native/client_seed_options.ex") <= 60

    assert source_line_count("../../lib/ferric_store/sdk/native/refresh_completion_queue.ex") <=
             170

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator/state.ex") <= 210

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator/pending_requests.ex") <=
             140

    assert source_line_count("../../lib/ferric_store/sdk/native/coordinator/state_events.ex") <=
             100
  end

  test "connection and event state are absent from the coordinator root" do
    state = %FerricStore.SDK.Native.Coordinator.State{}

    refute Map.has_key?(state, :connections)
    refute Map.has_key?(state, :connecting)
    refute Map.has_key?(state, :event_subscribers)
    refute Map.has_key?(state, :event_refcounts)
    refute Map.has_key?(state, :event_connection)
    refute Map.has_key?(state, :pending_requests)
    refute Map.has_key?(state, :lifecycle_monitors)
    refute Map.has_key?(state, :pending_batches)
    refute Map.has_key?(state, :batch_connection_queue)
    refute Map.has_key?(state, :topology)
    refute Map.has_key?(state, :refresh_operation)
    refute Map.has_key?(state, :event_subscriptions)
    refute Map.has_key?(state, :event_restore)
    refute Map.has_key?(state, :event_restore_attempt)
    refute Map.has_key?(state, :event_operation)
    refute Map.has_key?(state, :event_queue)

    assert %FerricStore.SDK.Native.ConnectionPool{} = state.connection_pool
    assert %{__struct__: FerricStore.SDK.Native.RequestRegistry} = state.request_registry
    assert %{__struct__: FerricStore.SDK.Native.LifecycleRegistry} = state.lifecycle_registry
    refute Map.has_key?(state.request_registry, :monitors)
    refute Map.has_key?(state.connection_pool.attempts, :key_by_monitor)
    assert %{__struct__: FerricStore.SDK.Native.BatchScheduler} = state.batch_scheduler
    assert %{__struct__: FerricStore.SDK.Native.TopologyManager} = state.topology_manager
    assert %{__struct__: FerricStore.SDK.Native.EventCoordinator} = state.event_coordinator
    assert %{__struct__: FerricStore.SDK.Native.EventRestore} = state.event_coordinator.restore
    refute Map.has_key?(state.event_coordinator, :restore_attempt)
  end

  test "the coordinator does not reach into state owned by focused managers" do
    source = source("../../lib/ferric_store/sdk/native/coordinator.ex")

    refute Regex.match?(~r/state\.event_coordinator\.[a-z_]+/, source)
    refute Regex.match?(~r/%\{state\.event_coordinator\s*\|/, source)
  end

  test "codec modules are transport independent", %{calls: calls} do
    assert_no_calls(calls,
      from: fn module ->
        module |> Atom.to_string() |> String.starts_with?("Elixir.FerricStore.Codec")
      end,
      to: [
        FerricStore.Client,
        FerricStore.Flow,
        FerricStore.Protocol,
        FerricStore.Queue,
        FerricStore.Workflow
      ]
    )
  end

  test "production modules do not contain debug IO calls", %{calls: calls} do
    assert_no_calls(calls,
      from: &ferric_store_module?/1,
      to: [IO],
      functions: [:puts, :inspect]
    )
  end

  test "production modules do not sleep in request paths", %{calls: calls} do
    assert_no_calls(calls,
      from: &ferric_store_module?/1,
      to: [Process],
      functions: [:sleep]
    )
  end
end
