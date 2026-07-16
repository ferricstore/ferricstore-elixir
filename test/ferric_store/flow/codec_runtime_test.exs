defmodule FerricStore.Flow.CodecRuntimeTest do
  use ExUnit.Case, async: false

  alias FerricStore.Flow

  defmodule RaisingCodec do
    @behaviour FerricStore.Codec
    def encode(_value), do: raise("encode failed")
    def decode(value), do: value
  end

  defmodule ThrowingCodec do
    @behaviour FerricStore.Codec
    def encode(_value), do: throw(:encode_failed)
    def decode(value), do: value
  end

  defmodule NonBinaryCodec do
    @behaviour FerricStore.Codec
    def encode(_value), do: :not_binary
    def decode(value), do: value
  end

  defmodule HardExitCodec do
    @behaviour FerricStore.Codec
    def encode(_value), do: Process.exit(self(), :kill)
    def decode(value), do: value
  end

  test "Flow write helpers contain codec failures as typed local errors" do
    for codec <- [RaisingCodec, ThrowingCodec, NonBinaryCodec],
        call <- [
          fn -> Flow.create(self(), "flow-1", type: "email", payload: "body", codec: codec) end,
          fn -> Flow.value_put(self(), "body", codec: codec) end,
          fn -> Flow.create_many(self(), [{"flow-1", "body"}], type: "email", codec: codec) end
        ] do
      assert {:error, %FerricStore.Error{raw: {:flow_codec_encode_failed, ^codec}}} = call.()
    end
  end

  test "direct payload construction raises a codec-specific boundary exception" do
    assert_raise FerricStore.Flow.CodecError, fn ->
      Flow.create_payload("flow-1", type: "email", payload: "body", codec: RaisingCodec)
    end
  end

  test "Flow write helpers contain hard codec exits for finite and infinite budgets" do
    for timeout <- [1_000, :infinity] do
      owner = self()

      {caller, monitor} =
        spawn_monitor(fn ->
          result =
            Flow.create(self(), "flow-1",
              type: "email",
              payload: "body",
              codec: HardExitCodec,
              timeout: timeout
            )

          send(owner, {:hard_codec_result, timeout, result})
        end)

      assert_receive {:hard_codec_result, ^timeout,
                      {:error,
                       %FerricStore.Error{
                         raw: {:flow_codec_encode_failed, HardExitCodec}
                       }}},
                     500

      assert_receive {:DOWN, ^monitor, :process, ^caller, :normal}, 500
    end
  end
end
