defmodule FerricStore.ProtocolTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol

  test "encodes and decodes native values" do
    value = %{"a" => 1, "b" => [nil, true, false, "bin", 1.5], "c" => %{"nested" => -2}}
    encoded = Protocol.encode_value(value)

    assert {:ok, decoded, ""} = Protocol.decode_value(encoded)
    assert decoded == value
  end

  test "encodes request frame header" do
    frame = Protocol.encode_request(Protocol.opcode(:ping), 7, %{}, lane_id: 3)

    assert <<"FSNP", 1, 0, 3::32, opcode::16, 7::64, body_len::32, body::binary>> = frame
    assert opcode == Protocol.opcode(:ping)
    assert body_len == byte_size(body)
  end

  test "builds generic command payload" do
    assert %{"command" => "SET", "args" => ["k", "v"]} =
             Protocol.command_payload(:set, ["k", "v"])
  end

  test "builds pipeline payload with decoded command bodies" do
    payload = Protocol.pipeline_payload([["SET", "k", "v"]])

    assert %{
             "commands" => [
               %{
                 "opcode" => _opcode,
                 "request_id" => 1,
                 "body" => %{"command" => "SET", "args" => ["k", "v"]}
               }
             ]
           } = payload
  end

  test "keeps direct native pipeline command bodies as maps" do
    payload =
      Protocol.pipeline_payload([%{opcode: Protocol.opcode(:flow_create), body: %{"id" => "f1"}}])

    assert %{
             "commands" => [
               %{
                 "opcode" => _opcode,
                 "request_id" => 1,
                 "body" => %{"id" => "f1"}
               }
             ]
           } = payload
  end

  test "decodes response body status" do
    body = <<0::16, Protocol.encode_value("OK")::binary>>
    assert {:ok, "OK"} = Protocol.decode_response_body(0, Protocol.opcode(:ping), body)

    error_body = <<1::16, Protocol.encode_value("ERR bad")::binary>>

    assert {:error, {1, "ERR bad"}} =
             Protocol.decode_response_body(0, Protocol.opcode(:ping), error_body)
  end
end
