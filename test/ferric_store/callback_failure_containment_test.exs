defmodule FerricStore.CallbackFailureContainmentTest do
  use ExUnit.Case, async: true

  alias FerricStore.{FailureFormatter, RequestContext}
  alias FerricStore.Protocol.{CommandSpec, Opcodes}

  alias FerricStore.SDK.Native.{BatchGroupPreparer, EndpointValidator}
  alias FerricStore.Test.{ExplodingError, RaisingInspect, ThrowingInspect}
  alias FerricStore.Transport.RequestEncoder

  test "exception rendering failures use the caller's bounded fallback" do
    assert FailureFormatter.exception_message(%ExplodingError{}, "callback failed") ==
             "callback failed"
  end

  test "batch callbacks cannot escape through a broken exception message implementation" do
    group = %{items: ["key"], indexes: [0], route: %{}}
    context = RequestContext.new([], 5_000)

    assert {:error, {:payload_builder_failed, {:error, "payload builder failed"}}} =
             BatchGroupPreparer.prepare(
               [group],
               fn _items -> raise ExplodingError end,
               &{:ok, &1},
               :retain_items,
               context
             )
  end

  test "endpoint callbacks cannot escape through a broken exception message implementation" do
    validator = fn _endpoint -> raise ExplodingError end

    assert {:error, {:endpoint_validator_failed, {:error, "endpoint validator failed"}}} =
             EndpointValidator.validate(validator, %{host: "cache.internal"})
  end

  test "request encoding contains Inspect implementations with broken exceptions" do
    assert {:error, {:encode_failed, "request encoding failed"}} =
             RequestEncoder.encode(
               FerricStore.Protocol.opcode(:set),
               1,
               1,
               %{"key" => "key", "value" => %RaisingInspect{}},
               1_024
             )
  end

  test "request encoding contains uninspectable thrown reasons" do
    assert {:error, {:encode_failed, "request encoding failed"}} =
             RequestEncoder.encode(
               FerricStore.Protocol.opcode(:set),
               1,
               1,
               %{"key" => "key", "value" => %ThrowingInspect{}},
               1_024
             )
  end

  test "command construction contains uninspectable thrown reasons" do
    assert {:error, {:invalid_command, "<unrenderable>"}} =
             FerricStore.Protocol.command_payload_result("PING", [],
               request_context: %{subject: %ThrowingInspect{}}
             )
  end

  test "public error conversion contains uninspectable raw reasons" do
    assert {:error, %FerricStore.Error{message: "<unrenderable>", raw: %ThrowingInspect{}}} =
             FerricStore.Result.error(%ThrowingInspect{})
  end

  test "bang connection errors contain uninspectable invalid options" do
    error =
      assert_raise FerricStore.Error, fn ->
        FerricStore.Client.connect!(max_connections: %ThrowingInspect{})
      end

    assert error.message == "connect failed: <unrenderable>"
  end

  test "consumer constructors contain uninspectable invalid configuration" do
    options_error =
      assert_raise ArgumentError, fn ->
        FerricStore.Queue.new(self(), "jobs", %ThrowingInspect{})
      end

    assert options_error.message ==
             "expected consumer options to be a keyword list, got: <unrenderable>"

    client_error =
      assert_raise ArgumentError, fn ->
        FerricStore.Workflow.new(%ThrowingInspect{}, "jobs")
      end

    assert client_error.message == "expected client to be a pid, got: <unrenderable>"
  end

  test "protocol descriptor lookup contains uninspectable unknown identifiers" do
    command_error =
      assert_raise ArgumentError, fn ->
        CommandSpec.fetch!(%ThrowingInspect{})
      end

    assert command_error.message == "unknown command: <unrenderable>"

    opcode_error =
      assert_raise ArgumentError, fn ->
        # Deliberately bypass the declared input type to exercise the runtime boundary.
        # credo:disable-for-next-line Credo.Check.Refactor.Apply
        apply(Opcodes, :fetch!, [%ThrowingInspect{}])
      end

    assert opcode_error.message == "unknown opcode: <unrenderable>"
  end
end
