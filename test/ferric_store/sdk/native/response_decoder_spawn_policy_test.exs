defmodule FerricStore.SDK.Native.ResponseDecoderSpawnPolicyTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.{ConnectionResponseDecoder, ResponseDecoderSpawnPolicy}

  test "preallocates only nontrivial Flow query decoders" do
    assert ResponseDecoderSpawnPolicy.options(Opcodes.flow_query(), 4_095) == []

    assert ResponseDecoderSpawnPolicy.options(Opcodes.flow_query(), 4_096) == [
             min_heap_size: 2_000
           ]

    assert ResponseDecoderSpawnPolicy.options(Opcodes.get(), 64 * 1_024) == []
    assert ResponseDecoderSpawnPolicy.options(Opcodes.flow_query(), :unknown) == []
  end

  test "the response decoder applies the selected minimum heap" do
    request_id = 71
    decode_token = make_ref()
    tag = make_ref()

    response = %{
      target: {:message, self(), tag},
      opcode: Opcodes.flow_query(),
      flags: 0,
      body: :binary.copy(<<0>>, 4_096),
      body_bytes: 4_096,
      max_response_bytes: 8_192,
      response_context: nil
    }

    worker = ConnectionResponseDecoder.start(self(), request_id, decode_token, response)

    assert_receive {:ferricstore_response_decoded, ^worker, ^request_id, ^decode_token,
                    {:response, :none}}

    assert {:heap_size, heap_words} = Process.info(worker, :heap_size)
    assert heap_words >= 2_000

    :ok = ConnectionResponseDecoder.deliver(worker, request_id, decode_token)
    assert_receive {:ferricstore_connection_response, _owner, ^tag, {:error, _reason}}
  end
end
