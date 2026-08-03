defmodule FerricStore.SDK.Native.PubSubCompactPipelineTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{Opcodes, PipelineRequest}
  alias FerricStore.SDK.Native.PipelinePreparer

  defp binary(value), do: <<byte_size(value)::unsigned-32, value::binary>>

  test "encodes homogeneous PUBLISH pipelines with mode 35" do
    request = %PipelineRequest{
      commands: [
        ["PUBLISH", "channel-a", "one"],
        ["publish", "channel-b", "two"]
      ],
      command_count: 2,
      options: [return: :compact]
    }

    assert {:ok, {:custom_payload, payload}} =
             PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true, true)

    assert IO.iodata_to_binary(payload) ==
             IO.iodata_to_binary([
               <<0x94, 35, 2::unsigned-32>>,
               binary("channel-a"),
               binary("one"),
               binary("channel-b"),
               binary("two")
             ])
  end

  test "preserves the generic pipeline for unsupported PUBLISH grammar" do
    for command <- [
          ["PUBLISH", "channel"],
          ["PUBLISH", "channel", "message", "extra"],
          ["PUBLISH", %{}, "message"],
          ["PUBLISH", "channel", %{}]
        ] do
      request = %PipelineRequest{
        commands: [command],
        command_count: 1,
        options: [return: :compact]
      }

      assert {:ok, payload} =
               PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true, true)

      assert is_map(payload)
    end
  end

  test "keeps the generic request unless the server advertises mode 35" do
    request = %PipelineRequest{
      commands: [["PUBLISH", "channel", "message"]],
      command_count: 1,
      options: [return: :compact]
    }

    assert {:ok, payload} =
             PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true, false)

    assert is_map(payload)
  end

  test "preserves public return and request-context semantics" do
    commands = [["PUBLISH", "channel", "message"]]

    for options <- [[], [return: :pairs], [return: :compact, request_context: %{trace: "id"}]] do
      request = %PipelineRequest{commands: commands, command_count: 1, options: options}

      assert {:ok, payload} =
               PipelinePreparer.prepare(Opcodes.pipeline(), request, 100, true, true)

      assert is_map(payload)
    end
  end
end
