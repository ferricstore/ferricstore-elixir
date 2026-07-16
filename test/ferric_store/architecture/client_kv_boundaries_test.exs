defmodule FerricStore.Architecture.ClientKvBoundariesTest do
  use FerricStore.Test.ArchitectureCase

  test "the public client is a facade over the canonical coordinator", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Client and
               call.callee_module == FerricStore.SDK.Native.Client and
               call.callee_function == :start_link
           end)

    assert_no_calls(calls,
      from: [FerricStore.Client],
      to: [FerricStore.Transport.Socket, FerricStore.Transport.FrameStream]
    )
  end

  test "the native public client and coordinator have one-way dependencies", %{calls: calls} do
    client_source = source("../../lib/ferric_store/sdk/native/client.ex")
    coordinator_source = source("../../lib/ferric_store/sdk/native/coordinator.ex")

    refute client_source =~ "use GenServer"
    assert coordinator_source =~ "use GenServer"

    refute File.exists?(
             Path.expand("../../../lib/ferric_store/sdk/native/client_api.ex", __DIR__)
           )

    assert_no_calls(calls,
      from: [
        FerricStore.SDK.Native.Coordinator,
        FerricStore.SDK.Native.CoordinatorShutdown,
        FerricStore.SDK.Native.Coordinator.State
      ],
      to: [FerricStore.SDK.Native.Client]
    )

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.Client and
               call.callee_module == FerricStore.SDK.Native.ClientRequests
           end)

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator],
      to: [FerricStore.SDK.Native.ClientSupervisor]
    )
  end

  test "event dispatcher clients share a neutral message protocol", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Transport.EventDispatcherClient],
      to: [FerricStore.Transport.EventDispatcher]
    )
  end

  test "client runtime processes are started only by the client supervisor", %{calls: calls} do
    state_source = source("../../lib/ferric_store/sdk/native/coordinator/state.ex")

    refute state_source =~ "DynamicSupervisor.start_link"
    refute state_source =~ "EventFanout.start_link"

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientLifecycleRequests and
               call.callee_module == FerricStore.SDK.Native.ClientSupervisor and
               call.callee_function == :start_link
           end)

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator.State],
      to: [DynamicSupervisor, FerricStore.SDK.Native.EventFanout],
      functions: [:start_link]
    )
  end

  test "public client calls resolve through the constant-time runtime endpoint", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.CoordinatorCall and
               call.callee_module == FerricStore.SDK.Native.ClientSupervisor and
               call.callee_function == :coordinator
           end)

    for {module, function} <- [
          {FerricStore.ClientIdentity, :endpoint},
          {:ets, :lookup}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.ClientEndpoint and
                 call.callee_module == module and call.callee_function == function
             end)
    end

    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.ClientSupervisor],
      to: [Supervisor],
      functions: [:which_children]
    )

    refute function_exported?(FerricStore.ClientIdentity, :mark, 1)
  end

  test "async waiting and cancellation are isolated from the public client facade", %{
    calls: calls
  } do
    for function <- [:await, :yield, :cancel] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Client and
                 call.callee_module == FerricStore.AsyncRequestRuntime and
                 call.callee_function == function
             end)
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.AsyncRequestRuntime and
               call.callee_module == FerricStore.SDK.Native.Client and
               call.callee_function == :cancel_async
           end)

    assert source_line_count("../../lib/ferric_store/client.ex") <= 220
    assert source_line_count("../../lib/ferric_store/async_request_runtime.ex") <= 160

    for function <- [:request, :async_request] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Client and
                 call.callee_module == FerricStore.NativeRequestRuntime and
                 call.callee_function == function
             end)
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.NativeRequestRuntime and
               call.callee_module == FerricStore.FlowRouting and
               call.callee_function == :resolve_payload
           end)

    assert source_line_count("../../lib/ferric_store/native_request_runtime.ex") <= 55
  end

  test "native request submission is separated from coordinator state transitions", %{
    calls: calls
  } do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.Client and
               call.callee_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_function == :request
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.SDK.Native.CoordinatorCall
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.Client and
               call.callee_module == FerricStore.SDK.Native.PipelineRequests and
               call.callee_function == :request
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/client.ex") <= 180
    refute source("../../lib/ferric_store/sdk/native/client_requests.ex") =~ "reply_module"
    refute source("../../lib/ferric_store/sdk/native/coordinator_reply.ex") =~ "reply_module"
  end

  test "public topology client types describe the pid-only runtime contract" do
    for relative_path <- [
          "../../lib/ferric_store/sdk/kv.ex",
          "../../lib/ferric_store/sdk/kv/scalar_commands.ex",
          "../../lib/ferric_store/sdk/kv/collection_commands.ex",
          "../../lib/ferric_store/sdk/management.ex",
          "../../lib/ferric_store/sdk/invocation.ex",
          "../../lib/ferric_store/sdk/flow.ex",
          "../../lib/ferric_store/sdk/admin.ex"
        ] do
      refute source(relative_path) =~ "GenServer.server()",
             "#{relative_path} advertises unsupported named-client semantics"
    end
  end

  test "invocation JSON validation is strict and isolated from its public input boundary", %{
    calls: calls
  } do
    for function <- [:validate_object, :encode] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.InvocationInput and
                 call.callee_module == FerricStore.SDK.InvocationJSON and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/invocation_input.ex") <= 80
    assert source_line_count("../../lib/ferric_store/sdk/invocation_json.ex") <= 60

    assert source_line_count("../../lib/ferric_store/sdk/invocation_json_input_validator.ex") <=
             50

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.InvocationJSON and
               call.callee_module == FerricStore.DeadlineTask and
               call.callee_function == :run
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.InvocationJSON and
               call.callee_module == FerricStore.SDK.InvocationJSONValidator and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.InvocationJSON and
               call.callee_module == FerricStore.SDK.InvocationJSONInputValidator and
               call.callee_function == :validate
           end)

    assert source_line_count("../../lib/ferric_store/sdk/invocation_options.ex") <= 40
    assert source_line_count("../../lib/ferric_store/sdk/invocation_json_validator.ex") <= 35
  end

  test "management input grammar is partitioned by value domain", %{calls: calls} do
    for {callee, function} <- [
          {FerricStore.SDK.ManagementRuleInput, :normalize},
          {FerricStore.SDK.ManagementPairInput, :args}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.ManagementInput and
                 call.callee_module == callee and call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/management_input.ex") <= 35
    assert source_line_count("../../lib/ferric_store/sdk/management_rule_input.ex") <= 100
    assert source_line_count("../../lib/ferric_store/sdk/management_pair_input.ex") <= 130
    assert source_line_count("../../lib/ferric_store/sdk/management_pair_normalizer.ex") <= 90
    assert source_line_count("../../lib/ferric_store/sdk/management_input_error.ex") <= 20
  end

  test "KV facade delegates collection admission and grouped response assembly", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV and
               call.callee_module == FerricStore.SDK.KV.CollectionCommands and
               call.callee_function == :del
           end)

    facade_boundaries = [
      {FerricStore.SDK.KV.MultiKeyCommands, :del},
      {FerricStore.SDK.KV.MultiKeyCommands, :mget},
      {FerricStore.SDK.KV.MultiKeyCommands, :mset}
    ]

    Enum.each(facade_boundaries, fn {module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.KV.CollectionCommands and
                 call.callee_module == module and call.callee_function == function
             end)
    end)

    multi_key_boundaries = [
      {FerricStore.SDK.KV.Input, :mset_pairs},
      {FerricStore.SDK.KV.BatchResults, :mget},
      {FerricStore.SDK.KV.BatchResults, :del},
      {FerricStore.SDK.KV.BatchResults, :mset}
    ]

    Enum.each(multi_key_boundaries, fn {module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.KV.MultiKeyCommands and
                 call.callee_module == module and call.callee_function == function
             end),
             "KV collection commands must delegate #{function} to #{inspect(module)}"
    end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.CollectionCommands and
               call.callee_module == FerricStore.SDK.KV.Input and
               call.callee_function == :zadd_items
           end)

    assert source_line_count("../../lib/ferric_store/sdk/kv.ex") <= 280

    for caller <- [
          FerricStore.SDK.KV.StringCommands,
          FerricStore.SDK.KV.LeaseCommands,
          FerricStore.SDK.KV.ComputeCommands,
          FerricStore.SDK.KV.HashReadCommands,
          FerricStore.SDK.KV.CollectionReadCommands,
          FerricStore.SDK.KV.SortedSetReadCommands,
          FerricStore.SDK.KV.CollectionCommands
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.KV.Response
             end),
             "#{inspect(caller)} must validate typed server replies"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.ComputeCommands and
               call.callee_module == FerricStore.SDK.KV.StructuredResponse
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.StructuredResponse and
               call.callee_module == FerricStore.SDK.KV.RateLimitResponse and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.MultiKeyCommands and
               call.callee_module == FerricStore.SDK.KV.MultiKeyPolicy and
               call.callee_function == :put
           end)

    for caller <- [FerricStore.SDK.KV.CollectionCommands, FerricStore.SDK.Native.KVRequests] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and call.callee_module == FerricStore.RouteKey and
                 call.callee_function == :validate
             end),
             "#{inspect(caller)} must use the canonical route-key validator"
    end

    assert source_line_count("../../lib/ferric_store/sdk/kv/response.ex") <= 140
    assert source_line_count("../../lib/ferric_store/sdk/kv/sorted_set_response.ex") <= 60
    assert source_line_count("../../lib/ferric_store/sdk/kv/score_response_parser.ex") <= 20
    assert source_line_count("../../lib/ferric_store/sdk/kv/structured_response.ex") <= 50
    assert source_line_count("../../lib/ferric_store/sdk/kv/rate_limit_response.ex") <= 40
    assert source_line_count("../../lib/ferric_store/sdk/kv/multi_key_policy.ex") <= 25
    assert source_line_count("../../lib/ferric_store/sdk/kv/collection_commands.ex") <= 210
    assert source_line_count("../../lib/ferric_store/sdk/kv/multi_key_commands.ex") <= 80

    assert source_line_count("../../lib/ferric_store/sdk/kv/scalar_commands.ex") <= 50

    for {callee, function} <- [
          {FerricStore.SDK.KV.StringCommands, :get},
          {FerricStore.SDK.KV.LeaseCommands, :lock},
          {FerricStore.SDK.KV.ComputeCommands, :ratelimit_add},
          {FerricStore.SDK.KV.HashReadCommands, :hget},
          {FerricStore.SDK.KV.CollectionReadCommands, :lpop},
          {FerricStore.SDK.KV.SortedSetReadCommands, :zrange}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.KV.ScalarCommands and
                 call.callee_module == callee and call.callee_function == function
             end)
    end

    for path <- [
          "../../lib/ferric_store/sdk/kv/string_commands.ex",
          "../../lib/ferric_store/sdk/kv/lease_commands.ex",
          "../../lib/ferric_store/sdk/kv/compute_commands.ex",
          "../../lib/ferric_store/sdk/kv/hash_read_commands.ex",
          "../../lib/ferric_store/sdk/kv/collection_read_commands.ex",
          "../../lib/ferric_store/sdk/kv/sorted_set_read_commands.ex"
        ] do
      assert source_line_count(path) <= 90
    end
  end

  test "KV MSET pair grammar is isolated from the general input facade", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.Input and
               call.callee_module == FerricStore.SDK.KV.MSetInput and
               call.callee_function == :pairs
           end)

    assert source_line_count("../../lib/ferric_store/sdk/kv/input.ex") <= 230
    assert source_line_count("../../lib/ferric_store/sdk/kv/mset_input.ex") <= 110
  end

  test "KV scalar wire-domain grammar is isolated from collection admission", %{calls: calls} do
    for function <- [
          :binary,
          :integer,
          :non_negative_integer,
          :positive_integer,
          :optional_boolean
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.KV.Input and
                 call.callee_module == FerricStore.SDK.KV.ScalarInput and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/sdk/kv/input.ex") <= 190
    assert source_line_count("../../lib/ferric_store/sdk/kv/scalar_input.ex") <= 90
  end

  test "typed KV transport is isolated from generic native client requests", %{calls: calls} do
    for caller <- [
          FerricStore.SDK.KV.StringCommands,
          FerricStore.SDK.KV.LeaseCommands,
          FerricStore.SDK.KV.ComputeCommands,
          FerricStore.SDK.KV.HashReadCommands,
          FerricStore.SDK.KV.CollectionReadCommands,
          FerricStore.SDK.KV.SortedSetReadCommands
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.KVRequests and
                 call.callee_function == :request_by_key
             end),
             "#{inspect(caller)} must delegate request_by_key/N to the typed KV gateway"
    end

    for {caller, function} <- [
          {FerricStore.SDK.KV.CollectionCommands, :request_by_key_with_count},
          {FerricStore.SDK.KV.MultiKeyCommands, :request_items}
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.SDK.Native.KVRequests and
                 call.callee_function == function
             end),
             "#{inspect(caller)} must delegate #{function}/N to the typed KV gateway"
    end

    assert_no_calls(calls,
      from: [
        FerricStore.SDK.KV.ScalarCommands,
        FerricStore.SDK.KV.StringCommands,
        FerricStore.SDK.KV.LeaseCommands,
        FerricStore.SDK.KV.ComputeCommands,
        FerricStore.SDK.KV.HashReadCommands,
        FerricStore.SDK.KV.CollectionReadCommands,
        FerricStore.SDK.KV.SortedSetReadCommands,
        FerricStore.SDK.KV.CollectionCommands,
        FerricStore.SDK.KV.MultiKeyCommands
      ],
      to: [FerricStore.SDK.Native.Client, FerricStore.SDK.Native.ClientRequests]
    )

    refute function_exported?(
             FerricStore.SDK.Native.ClientRequests,
             :request_kv_items_with_known_count,
             6
           )

    assert source_line_count("../../lib/ferric_store/sdk/native/client_requests.ex") <= 325
    assert source_line_count("../../lib/ferric_store/sdk/native/client_async_requests.ex") <= 140
    assert source_line_count("../../lib/ferric_store/sdk/native/event_inbox.ex") <= 50
    assert source_line_count("../../lib/ferric_store/sdk/native/kv_requests.ex") <= 180

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.KVRequests and
               call.callee_module == FerricStore.SDK.Native.KVBatchRequests and
               call.callee_function == :dispatch
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.SDK.Native.ClientAsyncRequests and
               call.callee_function == :request
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.SDK.Native.ClientLifecycleRequests and
               call.callee_function == :close
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.SDK.Native.ClientCommandRequests and
               call.callee_function == :command_exec
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/client_requests.ex") <= 210

    assert source_line_count("../../lib/ferric_store/sdk/native/client_lifecycle_requests.ex") <=
             125

    assert source_line_count("../../lib/ferric_store/sdk/native/client_command_requests.ex") <=
             120
  end

  test "generic native batch admission is owned outside the client request facade", %{
    calls: calls
  } do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ClientRequests and
               call.callee_module == FerricStore.SDK.Native.ClientBatchRequests and
               call.callee_function == :request_with_count
           end),
           "client request facade must delegate counted batches to ClientBatchRequests"

    assert source_line_count("../../lib/ferric_store/sdk/native/client_batch_requests.ex") <= 130
  end
end
