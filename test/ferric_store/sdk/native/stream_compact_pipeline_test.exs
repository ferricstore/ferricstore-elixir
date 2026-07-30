defmodule FerricStore.SDK.Native.StreamCompactPipelineTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{Opcodes, PipelineRequest}
  alias FerricStore.SDK.Native.PipelinePreparer

  defp binary(value), do: <<byte_size(value)::unsigned-32, value::binary>>

  test "encodes homogeneous auto-ID XADD pipelines with mode 34" do
    request = %PipelineRequest{
      commands: [
        ["XADD", "stream-a", "*", "field", "one"],
        ["xadd", "stream-b", "*", "first", "two", "second", "three"]
      ],
      command_count: 2,
      options: [return: :compact]
    }

    assert {:ok, {:custom_payload, payload}} =
             PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true)

    assert IO.iodata_to_binary(payload) ==
             IO.iodata_to_binary([
               <<0x94, 34, 2::unsigned-32>>,
               binary("stream-a"),
               <<1::unsigned-16>>,
               binary("field"),
               binary("one"),
               binary("stream-b"),
               <<2::unsigned-16>>,
               binary("first"),
               binary("two"),
               binary("second"),
               binary("three")
             ])
  end

  test "preserves the generic pipeline for unsupported XADD grammar" do
    for command <- [
          ["XADD", "stream", "1-0", "field", "value"],
          ["XADD", "stream", "NOMKSTREAM", "*", "field", "value"],
          ["XADD", "stream", "MAXLEN", "~", 100, "*", "field", "value"],
          ["XADD", "stream", "*"],
          ["XADD", "stream", "*", "field"],
          ["XADD", "stream", "*", "field", %{}]
        ] do
      request = %PipelineRequest{
        commands: [command],
        command_count: 1,
        options: [return: :compact]
      }

      assert {:ok, payload} = PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true)
      assert is_map(payload)
    end
  end

  test "preserves public return and request-context semantics" do
    commands = [["XADD", "stream", "*", "field", "value"]]

    for options <- [[], [return: :pairs], [return: :compact, request_context: %{trace: "id"}]] do
      request = %PipelineRequest{commands: commands, command_count: 1, options: options}
      assert {:ok, payload} = PipelinePreparer.prepare(Opcodes.pipeline(), request, 100)
      assert is_map(payload)
    end
  end

  test "keeps the generic request unless the server advertises mode 34" do
    request = %PipelineRequest{
      commands: [["XADD", "stream", "*", "field", "value"]],
      command_count: 1,
      options: [return: :compact]
    }

    assert {:ok, payload} = PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, false)
    assert is_map(payload)
  end
end
