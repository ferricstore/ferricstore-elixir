defmodule FerricStore.Architecture.ProtocolTransportTest do
  use FerricStore.Test.ArchitectureCase

  test "protocol codec stays below client and flow APIs", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Protocol],
      to: [FerricStore.Client, FerricStore.Flow, FerricStore.Queue, FerricStore.Workflow]
    )
  end

  test "protocol primitives are the only opcode metadata API", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Client],
      to: [FerricStore.Protocol.Opcodes]
    )

    refute Code.ensure_loaded?(FerricStore.SDK.Native.Opcodes)
  end

  test "command behavior has one canonical protocol descriptor" do
    assert Code.ensure_loaded?(CommandSpec)

    assert %{name: "WINDOW_UPDATE", lane: :control, read_only: false} =
             CommandSpec.fetch!(:window_update)

    assert %{lane: :control} = CommandSpec.fetch!(:goaway)
    assert %{lane: :control} = CommandSpec.fetch!(:event)

    assert %{batch: %{field: "items", type: :list}} =
             CommandSpec.fetch!(:zadd)

    assert CommandSpec.flow_property?("FLOW.SCHEDULE.CREATE", :schedule)
    assert CommandSpec.flow_property?("FLOW.GET", :state_id)

    refute source_contains?(
             "../../lib/ferric_store/sdk/native/coordinator.ex",
             "@control_lane_opcodes"
           )

    refute source_contains?("../../lib/ferric_store/request_limits.ex", "@batch_fields")
    refute source_contains?("../../lib/ferric_store/flow_routing.ex", "@schedule_commands")
    refute source_contains?("../../lib/ferric_store/protocol/opcodes.ex", "@read_operations")

    refute Regex.match?(
             ~r/commands\s*\|>\s*Enum\.with_index/,
             source("../../lib/ferric_store/protocol.ex")
           )

    assert source_line_count("../../lib/ferric_store/protocol/command_spec.ex") <= 150

    assert source_line_count("../../lib/ferric_store/protocol/command_spec/entries.ex") <= 160

    assert source_line_count("../../lib/ferric_store/protocol/command_spec/metadata.ex") <= 140

    assert source_line_count("../../lib/ferric_store/protocol/command_spec/flow_properties.ex") <=
             150
  end

  test "numeric opcode values are owned only by CommandSpec" do
    consumers = [
      "../../lib/ferric_store/protocol.ex",
      "../../lib/ferric_store/sdk/native/coordinator.ex",
      "../../lib/ferric_store/sdk/native/client_requests.ex",
      "../../lib/ferric_store/sdk/native/connection.ex",
      "../../lib/ferric_store/sdk/native/session_bootstrap.ex",
      "../../lib/ferric_store/transport/session_policy.ex"
    ]

    Enum.each(consumers, fn path ->
      source = source(path)

      refute Regex.match?(~r/@(?:op_[a-z0-9_]+|[a-z0-9_]+_opcode)\s+0x[0-9A-Fa-f]{4}\b/, source),
             "#{path} redeclares a numeric opcode"
    end)

    refute Regex.match?(
             ~r/def opcode\(:[a-z0-9_]+\)/,
             source("../../lib/ferric_store/protocol.ex")
           )
  end

  test "compact response dispatch does not form a protocol dependency cycle" do
    refute Code.ensure_loaded?(FerricStore.Protocol.CompactResponseContext)

    refute source_contains?(
             "../../lib/ferric_store/protocol/response_decoder.ex",
             "CompactResponseContext"
           )
  end

  test "socket client does not depend on high-level workflow modules", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Client],
      to: [FerricStore.Flow, FerricStore.Queue, FerricStore.Workflow]
    )
  end

  test "topology coordinator delegates socket IO to the connection layer", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Coordinator],
      to: [:gen_tcp, :ssl]
    )
  end

  test "native connection layer is independent of routing and public APIs", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.Connection],
      to: [
        FerricStore.SDK.Native.Client,
        FerricStore.SDK.Native.Coordinator,
        FerricStore.SDK.Native.Topology,
        FerricStore.SDK.KV,
        FerricStore.SDK.Flow,
        FerricStore.SDK.Management,
        FerricStore.SDK.Invocation
      ]
    )
  end

  test "high-level topology APIs use the coordinator instead of transport internals", %{
    calls: calls
  } do
    assert_no_calls(calls,
      from: &topology_api_module?/1,
      to: [FerricStore.SDK.Native.Connection]
    )
  end

  test "URL parsing is centralized below the public client facades", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Client, FerricStore.SDK.Native.Client],
      to: [URI]
    )
  end

  test "TLS verification policy is centralized below the native connection", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Transport.Socket and
               call.callee_module == FerricStore.Transport.TLS and
               call.callee_function == :options
           end),
           "FerricStore.Transport.Socket must use FerricStore.Transport.TLS.options/1"
  end

  test "raw socket IO exists only in the canonical connection session", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Client, FerricStore.SDK.Native.Connection],
      to: [:gen_tcp, :ssl]
    )

    for function <- [:connect, :set_active_once] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.ConnectionInitializer and
                 call.callee_module == FerricStore.Transport.Socket and
                 call.callee_function == function
             end),
             "canonical connection must use FerricStore.Transport.Socket.#{function}"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionShutdown and
               call.callee_module == FerricStore.Transport.Socket and
               call.callee_function == :close
           end),
           "connection shutdown must close the canonical socket"

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionEncodingWorker and
               call.callee_module == FerricStore.Transport.Socket and
               call.callee_function == :send
           end)
  end

  test "response frame buffering belongs to the connection socket runtime", %{calls: calls} do
    for function <- [:append, :next] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.SDK.Native.ConnectionSocketRuntime and
                 call.callee_module == FerricStore.Transport.FrameStream and
                 call.callee_function == function
             end),
             "connection socket runtime must use FerricStore.Transport.FrameStream.#{function}"
    end
  end

  test "response identity validation belongs to the canonical connection", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionFrameProcessor and
               call.callee_module == FerricStore.Transport.ResponseIdentity and
               call.callee_function == :validate
           end)
  end

  test "outbound encoding and request-size policy belong to the canonical connection", %{
    calls: calls
  } do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionEncodingWorker and
               call.callee_module == FerricStore.Transport.RequestEncoder and
               call.callee_function == :encode
           end)
  end

  test "deadline, GOAWAY, and request-id policy belong to the canonical connection", %{
    calls: calls
  } do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionEncodingWorker and
               call.callee_module == FerricStore.Transport.SessionPolicy and
               call.callee_function == :put_deadline
           end)

    for function <- [:next_request_id, :available_request_id] do
      assert Enum.any?(calls, fn call ->
               call.caller_module ==
                 FerricStore.SDK.Native.ConnectionPendingRegistration and
                 call.callee_module == FerricStore.Transport.SessionPolicy and
                 call.callee_function == function
             end),
             "outbound connection requests must use SessionPolicy.#{function}"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.ConnectionServerFrameRuntime and
               call.callee_module == FerricStore.Transport.SessionPolicy and
               call.callee_function == :server_frame_action
           end)
  end

  test "typed request context owns internal deadlines and admission metadata", %{calls: calls} do
    for caller <- [
          FerricStore.SDK.Native.CoordinatorSubmissionRuntime,
          FerricStore.SDK.Native.ClientRequests
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == caller and
                 call.callee_module == FerricStore.RequestContext
             end),
             "#{inspect(caller)} must use the typed request context"
    end

    for function <- [:new, :remaining, :cap] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.RequestContext and
                 call.callee_module == FerricStore.DeadlineBudget and
                 call.callee_function == function
             end)
    end

    refute File.exists?(Path.expand("../../../lib/ferric_store/request_deadline.ex", __DIR__))
    refute source_contains?("../../lib/ferric_store/request_limits.ex", "__batch_item_count__:")
    request_context_source = source("../../lib/ferric_store/request_context.ex")
    refute request_context_source =~ "def option(options"
    refute request_context_source =~ "def options(options"

    refute source_contains?(
             "../../lib/ferric_store/sdk/native/coordinator_timers.ex",
             "map() | keyword()"
           )

    refute source_contains?(
             "../../lib/ferric_store/sdk/native/batch_operation.ex",
             "opts: term()"
           )

    for relative_path <- [
          "../../lib/ferric_store/sdk/native/coordinator_request.ex",
          "../../lib/ferric_store/sdk/native/event_request.ex"
        ] do
      assert source_contains?(relative_path, "RequestContext.t()")
    end

    refute source_contains?(
             "../../lib/ferric_store/sdk/kv/collection_commands.ex",
             "opts \\\\ []"
           )
  end

  test "Flow capability metadata is partitioned by protocol domain", %{calls: calls} do
    assert source_line_count("../../lib/ferric_store/protocol/capability_optional_fields/flow.ex") <=
             40

    partitions = [
      {FerricStore.Protocol.CapabilityOptionalFields.FlowLifecycle,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_lifecycle.ex"},
      {FerricStore.Protocol.CapabilityOptionalFields.FlowOrchestration,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_orchestration.ex"},
      {FerricStore.Protocol.CapabilityOptionalFields.FlowQueries,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_queries.ex"},
      {FerricStore.Protocol.CapabilityOptionalFields.FlowPolicy,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_policy.ex"},
      {FerricStore.Protocol.CapabilityOptionalFields.FlowSchedules,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_schedules.ex"},
      {FerricStore.Protocol.CapabilityOptionalFields.FlowValues,
       "../../lib/ferric_store/protocol/capability_optional_fields/flow_values.ex"}
    ]

    Enum.each(partitions, fn {partition, path} ->
      assert source_line_count(path) <= 130

      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol.CapabilityOptionalFields.Flow and
                 call.callee_module == partition and call.callee_function == :all
             end)
    end)
  end
end
