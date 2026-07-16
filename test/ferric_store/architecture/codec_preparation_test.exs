defmodule FerricStore.Architecture.CodecPreparationTest do
  use FerricStore.Test.ArchitectureCase

  test "value and compact Flow codecs live behind the protocol facade", %{calls: calls} do
    boundaries = [
      {FerricStore.Protocol.ValueCodec, :encode},
      {FerricStore.Protocol.ValueCodec, :decode},
      {FerricStore.Protocol.FlowBatchCodec, :create_many_payload},
      {FerricStore.Protocol.FlowBatchCodec, :complete_many_payload},
      {FerricStore.Protocol.CommandPayload, :build},
      {FerricStore.Protocol.PipelinePayload, :build},
      {FerricStore.Protocol.FrameCodec, :encode_request_iodata}
    ]

    Enum.each(boundaries, fn {module, function} ->
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol and call.callee_module == module and
                 call.callee_function == function
             end),
             "protocol facade must delegate #{function} to #{inspect(module)}"
    end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.ValueCodec and
               call.callee_module == FerricStore.Protocol.ValueSizer and
               call.callee_function == :encoded_size
           end)

    for function <- [:encode, :encode_iodata] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol.ValueCodec and
                 call.callee_module == FerricStore.Protocol.ValueEncoder and
                 call.callee_function == function
             end),
             "value codec must delegate #{function}/N to the focused encoder"
    end

    for function <- [:encode_iodata, :encode_iodata_at_depth] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol.ValueCodec and
                 call.callee_module == FerricStore.Protocol.BoundedValueEncoder and
                 call.callee_function == function
             end),
             "value codec must delegate #{function}/N to the bounded encoder"
    end

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.RequestContextCodec and
               call.callee_module == FerricStore.Protocol.RequestContextScopes and
               call.callee_function == :normalize
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.RequestContextScopes and
               call.callee_module == FerricStore.Protocol.RequestContextScopeParser and
               call.callee_function == :normalize
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.FrameCodec and
               call.callee_module == FerricStore.Protocol.IodataSizer and
               call.callee_function == :bounded_length
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.CommandPayload and
               call.callee_module == FerricStore.Protocol.RequestContextCodec and
               call.callee_function == :put
           end)

    assert source_line_count("../../lib/ferric_store/protocol.ex") <= 160
    assert source_line_count("../../lib/ferric_store/protocol/frame_codec.ex") <= 150
    assert source_line_count("../../lib/ferric_store/protocol/command_payload.ex") <= 60
    assert source_line_count("../../lib/ferric_store/protocol/pipeline_payload.ex") <= 120
    assert source_line_count("../../lib/ferric_store/protocol/request_context_codec.ex") <= 80
    assert source_line_count("../../lib/ferric_store/protocol/iodata_sizer.ex") <= 55

    assert source_line_count("../../lib/ferric_store/protocol/request_context_scopes.ex") <= 50

    assert source_line_count("../../lib/ferric_store/protocol/request_context_scope_parser.ex") <=
             50

    assert source_line_count("../../lib/ferric_store/protocol/value_codec.ex") <= 120
    assert source_line_count("../../lib/ferric_store/protocol/value_encoder.ex") <= 145

    assert source_line_count("../../lib/ferric_store/protocol/bounded_value_encoder.ex") <= 190

    assert source_line_count("../../lib/ferric_store/protocol/value_sizer.ex") <= 160
  end

  test "Flow completion batches have a dedicated wire codec", %{calls: calls} do
    for function <- [:payload, :iodata_payload] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol.FlowBatchCodec and
                 call.callee_module == FerricStore.Protocol.FlowCompleteBatchCodec and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/protocol/flow_batch_codec.ex") <= 50

    for function <- [
          :create_many_payload,
          :create_many_iodata_payload,
          :create_many_ids_payload,
          :create_many_ids_iodata_payload
        ] do
      assert Enum.any?(calls, fn call ->
               call.caller_module == FerricStore.Protocol.FlowBatchCodec and
                 call.callee_module == FerricStore.Protocol.FlowCreateBatchCodec and
                 call.callee_function == function
             end)
    end

    assert source_line_count("../../lib/ferric_store/protocol/flow_create_batch_codec.ex") <= 145
    assert source_line_count("../../lib/ferric_store/protocol/flow_create_batch_items.ex") <= 105
    assert source_line_count("../../lib/ferric_store/protocol/flow_batch_fields.ex") <= 90

    assert source_line_count("../../lib/ferric_store/protocol/flow_complete_batch_codec.ex") <=
             150
  end

  test "trusted KV preparation owns compact routing and early wire budgets", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.SDK.Native.BatchCoordinator, FerricStore.SDK.Native.BatchRetry],
      to: [FerricStore.SDK.Native.KVBatchPreparer],
      functions: [:restore_items]
    )

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.KVBatchPreparer and
               call.callee_module == FerricStore.SDK.Native.BatchPreparer and
               call.callee_function == :prepare_compact
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.KVBatchPreparer and
               call.callee_module == FerricStore.SDK.Native.KVPayloadPreparer and
               call.callee_function == :prepare
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.KVPayloadPreparer and
               call.callee_module == FerricStore.Protocol.PreparedMap and
               call.callee_function == :prepare
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.KVPayloadPreparer and
               call.callee_module == FerricStore.Protocol.PreparedMSet and
               call.callee_function == :prepare
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Protocol.PreparedMSet and
               call.callee_module == FerricStore.Protocol.PreparedMap and
               call.callee_function == :prepare_encoded
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.Transport.SessionPolicy and
               call.callee_module == FerricStore.Protocol.PreparedMap and
               call.callee_function == :put_reserved
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.Native.BatchPreparer and
               call.callee_module == FerricStore.SDK.Native.BatchRouter and
               call.callee_function == :route
           end)

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_preparer.ex") <= 180

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_restored_preparation.ex") <=
             40

    assert source_line_count("../../lib/ferric_store/sdk/native/batch_router.ex") <= 130
    assert source_line_count("../../lib/ferric_store/sdk/native/batch_item_router.ex") <= 30
    assert source_line_count("../../lib/ferric_store/sdk/native/batch_map_collector.ex") <= 50
    assert source_line_count("../../lib/ferric_store/sdk/native/kv_batch_preparer.ex") <= 200
    assert source_line_count("../../lib/ferric_store/sdk/native/kv_payload_preparer.ex") <= 80
    assert source_line_count("../../lib/ferric_store/protocol/prepared_map.ex") <= 200
    assert source_line_count("../../lib/ferric_store/protocol/prepared_mset.ex") <= 110

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.BatchResults and
               call.callee_module == FerricStore.SDK.KV.GroupedWriteResults and
               call.callee_function in [:del, :mset]
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.BatchResults and
               call.callee_module == FerricStore.SDK.KV.MGetGroupValidation and
               call.callee_function == :size_error
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.BatchResults and
               call.callee_module == FerricStore.SDK.KV.ResultCount and
               call.callee_function == :validate
           end)

    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.BatchResults and
               call.callee_module == FerricStore.SDK.KV.MGetMerge and
               call.callee_function == :merge
           end)

    assert source_line_count("../../lib/ferric_store/sdk/kv/batch_results.ex") <= 170
    assert source_line_count("../../lib/ferric_store/sdk/kv/mget_merge.ex") <= 150

    assert source_line_count("../../lib/ferric_store/sdk/kv/mget_group_validation.ex") <= 40
    assert source_line_count("../../lib/ferric_store/sdk/kv/result_count.ex") <= 25

    assert source_line_count("../../lib/ferric_store/sdk/kv/grouped_write_results.ex") <= 120

    refute File.exists?(
             Path.expand(
               "../../../lib/ferric_store/sdk/native/kv_payload_sizer.ex",
               __DIR__
             )
           )
  end

  test "KV input facade delegates sorted-set normalization", %{calls: calls} do
    assert Enum.any?(calls, fn call ->
             call.caller_module == FerricStore.SDK.KV.Input and
               call.callee_module == FerricStore.SDK.KV.SortedSetInput and
               call.callee_function == :zadd_items
           end),
           "KV.Input must delegate ZADD normalization to SortedSetInput"

    assert source_line_count("../../lib/ferric_store/sdk/kv/input.ex") <= 300
    assert source_line_count("../../lib/ferric_store/sdk/kv/sorted_set_input.ex") <= 110
  end
end
