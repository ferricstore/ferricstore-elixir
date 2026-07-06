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

  test "exposes state metadata policy and search opcodes" do
    assert Protocol.opcode(:flow_policy_set) == 0x021E
    assert Protocol.opcode(:flow_policy_get) == 0x021F
    assert Protocol.opcode(:flow_search) == 0x0230
  end

  test "resolves newer native opcodes through the complete SDK table" do
    assert Protocol.opcode(:route_batch) == 0x000F
    assert Protocol.opcode(:flow_schedule_create) == 0x0225
    assert Protocol.opcode(:flow_budget_release) == 0x0258
    assert Protocol.opcode(:cluster_health) == 0x0301
    assert Protocol.opcode(:ferricstore_blobgc) == 0x0310
  end

  test "encodes custom payload request frame" do
    frame =
      Protocol.encode_request(
        Protocol.opcode(:flow_create_many),
        7,
        Protocol.custom_payload(<<0x90>>)
      )

    assert <<"FSNP", 1, flags, 1::32, opcode::16, 7::64, 1::32, 0x90>> = frame
    assert Bitwise.band(flags, Protocol.flag_custom_payload()) != 0
    assert opcode == Protocol.opcode(:flow_create_many)
  end

  test "builds generic command payload" do
    assert %{"command" => "SET", "args" => ["k", "v"]} =
             Protocol.command_payload(:set, ["k", "v"])
  end

  test "builds generic command payload with request context" do
    assert %{
             "command" => "SET",
             "args" => ["k", "v"],
             "request_context" => %{
               "subject" => "client-1",
               "tenant" => "t1",
               "scopes" => ["tenant:t1:write"]
             }
           } =
             Protocol.command_payload(:set, ["k", "v"],
               request_context: %{
                 subject: "client-1",
                 tenant: "t1",
                 scopes: ["tenant:t1:write", nil]
               }
             )
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

  test "builds pipeline payload with top-level request context" do
    payload =
      Protocol.pipeline_payload([["SET", "k", "v"]],
        request_context: %{
          "subject" => "client-1",
          "tenant" => "t1",
          "scopes" => "tenant:t1:write invocation:create:*"
        }
      )

    assert payload["request_context"] == %{
             "subject" => "client-1",
             "tenant" => "t1",
             "scopes" => ["tenant:t1:write", "invocation:create:*"]
           }
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

  test "decodes compact direct KV responses" do
    assert {:ok, "OK"} =
             Protocol.decode_response_body(0, Protocol.opcode(:set), <<0::16, 0x81, 1::32>>)

    assert {:ok, "value"} =
             Protocol.decode_response_body(
               0,
               Protocol.opcode(:get),
               <<0::16, 0x82, 1, 5::32, "value">>
             )

    assert {:ok, nil} =
             Protocol.decode_response_body(0, Protocol.opcode(:get), <<0::16, 0x82, 0>>)
  end

  test "decodes compact Flow value mget responses" do
    body = <<0::16, 0x83, 2::32, 1, 5::32, "value", 0>>

    assert {:ok, ["value", nil]} =
             Protocol.decode_response_body(0, Protocol.opcode(:flow_value_mget), body)
  end

  test "encodes compact flow create many payload" do
    assert {:ok, <<0x90, _rest::binary>>} =
             Protocol.compact_flow_create_many_payload(%{
               "type" => "email",
               "state" => "queued",
               "now_ms" => 10,
               "run_at_ms" => 10,
               "independent" => true,
               "return" => "OK_ON_SUCCESS",
               "items" => [["flow-1", ""]]
             })
  end

  test "does not compact create many payloads with richer mutation fields" do
    assert :error =
             Protocol.compact_flow_create_many_payload(%{
               "type" => "email",
               "state" => "queued",
               "now_ms" => 10,
               "run_at_ms" => 10,
               "items" => [["flow-1", ""]],
               "state_meta" => %{"version" => 1}
             })
  end

  test "direct compact flow create many ids payload matches generic compact payload" do
    payload = %{
      "type" => "email",
      "state" => "queued",
      "partition_key" => "p1",
      "now_ms" => 10,
      "run_at_ms" => 10,
      "independent" => true,
      "return" => "OK_ON_SUCCESS",
      "items" => [["flow-1", ""], ["flow-2", ""]]
    }

    assert Protocol.compact_flow_create_many_payload(payload) ==
             Protocol.compact_flow_create_many_ids_payload(
               "email",
               "queued",
               "p1",
               ["flow-1", "flow-2"],
               now_ms: 10,
               run_at_ms: 10,
               independent: true,
               return_ok_on_success: true
             )
  end

  test "encodes compact flow complete many payload" do
    assert {:ok, <<0x93, _rest::binary>>} =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 10,
               "return" => "OK_ON_SUCCESS",
               "items" => [["flow-1", "p1", "lease-1", 10]]
             })
  end

  test "does not compact complete many payloads with shared result or metadata" do
    assert :error =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 10,
               "items" => [["flow-1", "p1", "lease-1", 10]],
               "result" => "done"
             })

    assert :error =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 10,
               "items" => [["flow-1", "p1", "lease-1", 10]],
               "state_meta" => %{"version" => 2}
             })
  end

  test "decodes compact pipeline response" do
    body = <<0::16, 0x95, 2::32, 0, 1, 2::32, "OK", 0, 1, 5::32, "value">>

    assert {:ok, [["ok", "OK"], ["ok", "value"]]} =
             Protocol.decode_response_body(0, Protocol.opcode(:pipeline), body)
  end

  test "decodes compact claim_due response" do
    body =
      <<
        0::16,
        0x80,
        2::32,
        6::32,
        "flow-1",
        0xFFFF_FFFF::32,
        7::32,
        "lease-1",
        10::signed-64,
        6::32,
        "flow-2",
        2::32,
        "p1",
        7::32,
        "lease-2",
        11::signed-64
      >>

    assert {:ok, [["flow-1", nil, "lease-1", 10], ["flow-2", "p1", "lease-2", 11]]} =
             Protocol.decode_response_body(0, Protocol.opcode(:flow_claim_due), body)
  end

  test "decodes compact claim_due response with attributes" do
    attrs = Protocol.encode_value(%{"tenant" => "acme"})

    body =
      IO.iodata_to_binary([
        <<0::16, 0x80, 1::32, 6::32>>,
        "flow-1",
        <<0xFFFF_FFFF::32, 7::32>>,
        "lease-1",
        <<10::signed-64>>,
        attrs
      ])

    assert {:ok, [["flow-1", nil, "lease-1", 10, %{"tenant" => "acme"}]]} =
             Protocol.decode_response_body(0, Protocol.opcode(:flow_claim_due), body)
  end
end
