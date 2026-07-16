defmodule FerricStore.ProtocolTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Protocol.{PreparedMap, ValueCodec}
  alias FerricStore.SDK.Native.Codec, as: NativeCodec
  alias FerricStore.Test.ExplodingString
  alias FerricStore.Transport.RequestEncoder

  test "encodes and decodes native values" do
    value = %{"a" => 1, "b" => [nil, true, false, "bin", 1.5], "c" => %{"nested" => -2}}
    encoded = Protocol.encode_value(value)

    assert {:ok, decoded, ""} = Protocol.decode_value(encoded)
    assert decoded == value
  end

  test "computes bounded wire sizes without materializing encoded values" do
    values = [
      nil,
      true,
      false,
      -42,
      "binary",
      :atom,
      1.5,
      [nil, "nested", %{count: 2}],
      {:tuple, 7},
      %{"a" => 1, b: [false, "value"]}
    ]

    Enum.each(values, fn value ->
      encoded_size = value |> ValueCodec.encode_iodata() |> IO.iodata_length()

      assert {:ok, ^encoded_size} = ValueCodec.encoded_size(value, encoded_size)
      assert {:error, :too_large} = ValueCodec.encoded_size(value, encoded_size - 1)
    end)
  end

  test "transport request encoding preserves iodata until the socket boundary" do
    payload = %{"keys" => Enum.map(1..1_000, &"key-#{&1}")}

    assert {:ok, frame} = RequestEncoder.encode(0x0104, 1, 7, payload, 1_000_000)
    assert is_list(frame)

    assert IO.iodata_to_binary(frame) ==
             Protocol.encode_request(0x0104, 7, payload, lane_id: 1, max_body_bytes: 1_000_000)
  end

  test "trusted compact batch counts must still match the encoded collection" do
    create_payload = %{
      "type" => "email",
      "state" => "queued",
      "now_ms" => 10,
      "run_at_ms" => 10,
      "items" => [["one", ""], ["two", ""]]
    }

    complete_payload = %{
      "now_ms" => 10,
      "items" => [["one", "lease", 1], ["two", "lease", 2]]
    }

    assert :error = Protocol.compact_flow_create_many_iodata_payload(create_payload, 1)
    assert :error = Protocol.compact_flow_complete_many_iodata_payload(complete_payload, 1)
  end

  test "decoded fields do not retain an unrelated response binary" do
    retained_value = String.duplicate("s", 65)

    encoded =
      Protocol.encode_value(%{
        "retained" => retained_value,
        "discarded" => String.duplicate("x", 5_000_000)
      })

    assert {:ok, decoded, ""} = Protocol.decode_value(encoded)
    decoded_value = decoded["retained"]

    assert decoded_value == retained_value
    assert :binary.referenced_byte_size(decoded_value) <= byte_size(decoded_value) * 2
  end

  test "rejects integers outside the signed 64-bit wire domain" do
    min = -9_223_372_036_854_775_808
    max = 9_223_372_036_854_775_807

    assert {:ok, ^min, ""} = min |> Protocol.encode_value() |> Protocol.decode_value()
    assert {:ok, ^max, ""} = max |> Protocol.encode_value() |> Protocol.decode_value()

    assert_raise ArgumentError, ~r/signed 64-bit/, fn ->
      Protocol.encode_value(max + 1)
    end

    assert_raise ArgumentError, ~r/signed 64-bit/, fn ->
      Protocol.encode_value(min - 1)
    end
  end

  test "rejects map keys that collide after wire normalization" do
    assert_raise ArgumentError, ~r/duplicate normalized map key.*a/, fn ->
      Protocol.encode_value(%{:a => 2, "a" => 1})
    end
  end

  test "contains failures from user map-key string conversions in every encoder" do
    value = %{%ExplodingString{} => "value"}

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      Protocol.encode_value(value)
    end

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      ValueCodec.encode_iodata(value, 1_024)
    end

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      ValueCodec.encoded_size(value, 1_024)
    end

    assert_raise ArgumentError, ~r/cannot encode native map key/, fn ->
      PreparedMap.prepare(value, 1_024)
    end
  end

  test "rejects duplicate map keys received from the wire" do
    encoded =
      <<6, 2::32, 1::32, "a", Protocol.encode_value(1)::binary, 1::32, "a",
        Protocol.encode_value(2)::binary>>

    assert {:error, {:duplicate_map_key, %{bytes: 1}}} = Protocol.decode_value(encoded)
  end

  test "rejects out-of-range request header fields instead of truncating them" do
    assert_raise ArgumentError, ~r/opcode/, fn ->
      Protocol.encode_request(0x1_0000, 1, %{})
    end

    assert_raise ArgumentError, ~r/lane_id/, fn ->
      Protocol.encode_request(0x0101, 1, %{}, lane_id: -1)
    end
  end

  test "compact flow encoders reject signed 64-bit overflow instead of wrapping" do
    too_large = 9_223_372_036_854_775_808

    assert :error =
             Protocol.compact_flow_create_many_ids_payload(
               "email",
               "queued",
               nil,
               ["flow-1"],
               now_ms: too_large,
               run_at_ms: 0
             )

    assert :error =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 0,
               "items" => [["flow-1", "lease", too_large]]
             })
  end

  test "compact flow encoders reject collections above the protocol limit" do
    too_many_ids = List.duplicate("flow", 100_001)
    too_many_create_items = List.duplicate(["flow", "payload"], 100_001)
    too_many_complete_items = List.duplicate(["flow", "lease", 1], 100_001)

    assert :error =
             Protocol.compact_flow_create_many_ids_payload(
               "email",
               "queued",
               nil,
               too_many_ids,
               now_ms: 0,
               run_at_ms: 0
             )

    assert :error =
             Protocol.compact_flow_create_many_payload(%{
               "type" => "email",
               "state" => "queued",
               "now_ms" => 0,
               "run_at_ms" => 0,
               "items" => too_many_create_items
             })

    assert :error =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 0,
               "items" => too_many_complete_items
             })
  end

  test "custom payload iodata is size-checked before request flattening" do
    body = ["prefix", [String.duplicate("x", 64)]]

    assert_raise Protocol.RequestTooLargeError, fn ->
      Protocol.encode_request(
        Protocol.opcode(:flow_create_many),
        1,
        Protocol.custom_payload(body),
        max_body_bytes: 32
      )
    end
  end

  test "request encoding stops traversing values as soon as the byte budget is exhausted" do
    shared = List.duplicate(nil, 1_000)
    payload = %{"items" => List.duplicate(shared, 1_000)}
    {:reductions, before} = Process.info(self(), :reductions)

    assert_raise Protocol.RequestTooLargeError, fn ->
      Protocol.encode_request(Protocol.opcode(:set), 1, payload, max_body_bytes: 64)
    end

    {:reductions, after_encoding} = Process.info(self(), :reductions)
    assert after_encoding - before < 50_000
  end

  test "list values are counted while encoding instead of in a separate pass" do
    value = List.duplicate(nil, 100_000)
    {:reductions, before_encoding} = Process.info(self(), :reductions)
    encoded = Protocol.encode_value(value)
    {:reductions, after_encoding} = Process.info(self(), :reductions)

    assert byte_size(encoded) == 100_005
    assert after_encoding - before_encoding < 500_000
  end

  test "the canonical client uses one native value and request encoding" do
    values = [nil, true, false, -42, 1.5, "value", [1, "two"], {"tuple", 9}, %{"k" => 3}]

    for value <- values do
      encoded = Protocol.encode_value(value)

      assert NativeCodec.encode_value(value) == encoded
      assert NativeCodec.decode_value(encoded) == Protocol.decode_value(encoded)
    end

    payload = %{"key" => "same-wire-format"}

    assert NativeCodec.encode_frame(0x0101, 7, 99, payload) ==
             Protocol.encode_request(0x0101, 99, payload, lane_id: 7)
  end

  test "compressed responses are decoded within an explicit output bound" do
    value = String.duplicate("compressible", 2_000)
    body = <<0::unsigned-16, Protocol.encode_value(value)::binary>>
    compressed = :zlib.compress(body)

    assert {:ok, ^value} = NativeCodec.decode_response(0x0101, 0x08, compressed, byte_size(body))

    assert {:error, :decompressed_response_too_large} =
             NativeCodec.decode_response(0x0101, 0x08, compressed, 1_024)
  end

  test "the public protocol decoder supports bounded compressed responses" do
    value = String.duplicate("compressible", 2_000)
    body = <<0::unsigned-16, Protocol.encode_value(value)::binary>>
    compressed = :zlib.compress(body)

    assert {:ok, ^value} =
             Protocol.decode_response_body(
               Protocol.flag_compressed(),
               Protocol.opcode(:get),
               compressed
             )
  end

  test "compressed responses reject bytes after the zlib stream" do
    body = <<0::unsigned-16>> <> Protocol.encode_value("value")
    compressed = :zlib.compress(body)

    assert {:error, :invalid_compressed_payload} =
             NativeCodec.decode_response(0x0101, 0x08, compressed <> "trailing", 1_024)
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
    payload = Protocol.custom_payload(<<0x90>>)
    frame = Protocol.encode_request(Protocol.opcode(:flow_create_many), 7, payload)

    assert <<"FSNP", 1, flags, 1::32, opcode::16, 7::64, 1::32, 0x90>> = frame
    assert Bitwise.band(flags, Protocol.flag_custom_payload()) != 0
    assert opcode == Protocol.opcode(:flow_create_many)
  end

  test "builds generic command payload" do
    assert %{"command" => "SET", "args" => ["k", "v"]} =
             Protocol.command_payload("set", ["k", "v"])
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
             Protocol.command_payload("set", ["k", "v"],
               request_context: %{
                 subject: "client-1",
                 tenant: "t1",
                 scopes: ["tenant:t1:write", nil]
               }
             )
  end

  test "generic command payloads reject legacy and unbounded command names" do
    for {command, reason} <- [
          {:set, :expected_binary},
          {"", :empty},
          {<<0xFF>>, :invalid_utf8},
          {String.duplicate("x", 1_025), :too_long}
        ] do
      assert {:error, {:invalid_command, %{reason: ^reason, value: ^command}}} =
               Protocol.command_payload_result(command, [])
    end
  end

  test "request context rejects ambiguous atom and string fields" do
    request_context = %{"subject" => "string-subject", subject: "atom-subject"}

    for result <- [
          Protocol.command_payload_result("PING", [], request_context: request_context),
          Protocol.pipeline_payload_result([["PING"]], request_context: request_context)
        ] do
      assert {:error, {:invalid_command, message}} = result
      assert message =~ ~s(duplicate request context field "subject")
    end
  end

  test "request context rejects malformed identity fields instead of dropping them" do
    contexts = [
      {%{subject: 123}, "subject"},
      {%{tenant: false}, "tenant"},
      {%{scopes: %{admin: true}}, "scopes"}
    ]

    for {request_context, field} <- contexts,
        result <- [
          Protocol.command_payload_result("PING", [], request_context: request_context),
          Protocol.pipeline_payload_result([["PING"]], request_context: request_context)
        ] do
      assert {:error, {:invalid_command, message}} = result
      assert message =~ "request context field #{field}"
    end
  end

  test "request context rejects non-map containers instead of dropping them" do
    for request_context <- [:invalid, [subject: "client-1"], "client-1"],
        result <- [
          Protocol.command_payload_result("PING", [], request_context: request_context),
          Protocol.pipeline_payload_result([["PING"]], request_context: request_context)
        ] do
      assert {:error, {:invalid_command, message}} = result
      assert message =~ "request context must be a map"
    end
  end

  test "request context scope lists are bounded before filtering and deduplication" do
    scopes = List.duplicate("same-scope", 100_001)

    assert {:error, {:invalid_command, message}} =
             Protocol.command_payload_result("PING", [], request_context: %{scopes: scopes})

    assert message =~ "request context scopes exceed 100000 items"
  end

  test "request context scope strings stop at the item limit without splitting the full input" do
    parent = self()
    scopes = String.duplicate("same-scope ", 1_000_000)

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        Process.flag(:max_heap_size, %{size: 1_000_000, kill: true, error_logger: false})

        result =
          Protocol.command_payload_result("PING", [], request_context: %{scopes: scopes})

        send(parent, {self(), result})
      end)

    assert_receive {^pid, {:error, {:invalid_command, message}}}, 2_000
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :normal}, 2_000

    assert message =~ "request context scopes exceed 100000 items"
  end

  test "request context scope strings preserve comma and space token semantics" do
    cases = [
      {"", nil},
      {" ,  , ", nil},
      {"read", ["read"]},
      {"read,write read,,admin ", ["read", "write", "admin"]},
      {"scope\twith-tab another", ["scope\twith-tab", "another"]}
    ]

    for {scopes, expected} <- cases do
      assert {:ok, payload} =
               Protocol.command_payload_result("PING", [], request_context: %{scopes: scopes})

      assert get_in(payload, ["request_context", "scopes"]) == expected
    end
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

  test "pipeline payloads reject legacy scalar commands and lossy typed maps" do
    get_opcode = Protocol.opcode(:get)

    invalid_commands = [
      {"PING", :expected_nonempty_list_or_typed_map},
      {:ping, :expected_nonempty_list_or_typed_map},
      {[], :expected_nonempty_list_or_typed_map},
      {[""], :invalid_command_name},
      {[:ping], :invalid_command_name},
      {[<<0xFF>>], :invalid_command_name},
      {["PING", "arg" | :invalid_tail], :invalid_command_arguments},
      {%{opcode: get_opcode, body: %{}, typo: true}, :unsupported_fields},
      {%{:opcode => get_opcode, "opcode" => get_opcode, body: %{}}, :duplicate_field},
      {%{opcode: -1, body: %{}}, :invalid_opcode},
      {%{opcode: get_opcode, body: [], lane_id: 1}, :invalid_body},
      {%{opcode: get_opcode, body: %{}, lane_id: -1}, :invalid_lane_id},
      {%{opcode: get_opcode, body: %{}, request_id: -1}, :invalid_request_id},
      {%{opcode: Protocol.opcode(:hello), body: %{}}, :control_opcode}
    ]

    Enum.each(invalid_commands, fn {command, reason} ->
      assert {:error, {:invalid_pipeline_command, %{index: 0, reason: ^reason}}} =
               Protocol.pipeline_payload_result([command])
    end)
  end

  test "pipeline raw argument admission is bounded" do
    args = List.duplicate("arg", 100_001)

    assert {:error, {:invalid_pipeline_command, %{index: 0, reason: :too_many_command_arguments}}} =
             Protocol.pipeline_payload_result([["ECHO" | args]])
  end

  test "pipeline validation reports the failing index and preserves explicit typed metadata" do
    get_opcode = Protocol.opcode(:get)

    assert {:ok,
            %{
              "commands" => [
                %{
                  "opcode" => ^get_opcode,
                  "body" => %{"key" => "key"},
                  "lane_id" => 0,
                  "request_id" => 0
                }
              ]
            }} =
             Protocol.pipeline_payload_result([
               %{"opcode" => get_opcode, body: %{"key" => "key"}, lane_id: 0, request_id: 0}
             ])

    assert {:error,
            {:invalid_pipeline_command, %{index: 1, reason: :expected_nonempty_list_or_typed_map}}} =
             Protocol.pipeline_payload_result([["PING"], "GET"])
  end

  test "pipeline request ids are assigned without an indexed intermediate list" do
    commands = List.duplicate(["PING"], 100_000)
    {:reductions, before_build} = Process.info(self(), :reductions)
    payload = Protocol.pipeline_payload(commands)
    {:reductions, after_build} = Process.info(self(), :reductions)

    assert length(payload["commands"]) == 100_000
    assert after_build - before_build < 3_500_000
  end

  test "pipeline size rejection does not preprocess an unbounded argument tail" do
    short_tail = List.duplicate(:value, 10_000)
    long_tail = List.duplicate(:value, 100_000)

    short_reductions = rejected_pipeline_reductions(short_tail)
    long_reductions = rejected_pipeline_reductions(long_tail)

    assert long_reductions < short_reductions * 3
  end

  test "pipeline arguments are normalized once by the wire encoder" do
    payload =
      Protocol.pipeline_payload([
        ["ECHO", :ready, {:tuple, 1}, %{answer: :yes}]
      ])

    assert {:ok, frame} =
             RequestEncoder.encode(
               Protocol.opcode(:pipeline),
               1,
               7,
               payload,
               10_000
             )

    frame = IO.iodata_to_binary(frame)

    assert <<"FSNP", 1, _flags, 1::32, _opcode::16, 7::64, body_size::32,
             body::binary-size(body_size)>> = frame

    assert {:ok, decoded, ""} = Protocol.decode_value(body)

    assert get_in(decoded, ["commands", Access.at(0), "body", "args"]) == [
             "ready",
             ["tuple", 1],
             %{"answer" => "yes"}
           ]
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

  test "native response errors preserve unknown status codes" do
    body = <<65_535::16, Protocol.encode_value("future status")::binary>>

    assert {:error, {:unknown_status, 65_535, "future status"}} =
             NativeCodec.decode_response(Protocol.opcode(:ping), 0, body)
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

  test "rejects impossible compact success counts for scalar writes" do
    for opcode <- [Protocol.opcode(:set), Protocol.opcode(:mset)], count <- [0, 2] do
      assert {:error, :invalid_compact_scalar_count} =
               Protocol.decode_compact_response_payload(opcode, <<0x81, count::32>>)
    end
  end

  test "decodes compact success lists for every bulk Flow mutation emitted by the server" do
    opcodes =
      Enum.map(
        [
          :flow_create_many,
          :flow_complete_many,
          :flow_retry_many,
          :flow_fail_many,
          :flow_cancel_many
        ],
        &Protocol.opcode/1
      )

    for opcode <- opcodes do
      assert {:ok, ["OK", "OK", "OK"]} =
               Protocol.decode_compact_response_payload(opcode, <<0x81, 3::32>>)
    end
  end

  test "rejects obsolete top-level compact item tags for pipeline responses" do
    pipeline = Protocol.opcode(:pipeline)

    assert {:error, :invalid_value} =
             Protocol.decode_compact_response_payload(pipeline, <<0x83, 0::32>>)

    assert {:error, :invalid_value} =
             Protocol.decode_compact_response_payload(pipeline, <<0x80, 0::32>>)
  end

  test "decodes compact Flow value mget responses" do
    body = <<0::16, 0x83, 2::32, 1, 5::32, "value", 0>>

    assert {:ok, ["value", nil]} =
             Protocol.decode_response_body(0, Protocol.opcode(:flow_value_mget), body)
  end

  test "decodes current structured compact response tags" do
    record =
      IO.iodata_to_binary([
        <<0x84, 4::32, 1>>,
        Protocol.encode_value("flow-1"),
        <<2>>,
        Protocol.encode_value("email"),
        <<3>>,
        Protocol.encode_value("queued"),
        <<41>>,
        Protocol.encode_value(%{"tenant" => "acme"})
      ])

    record_list = <<0x85, 1::32, record::binary>>
    list_list = <<0x86, 2::32, 2::32, 1::32, "a", 2::32, "bb", 0::32>>
    map_list = <<0x87, 1::32, 1::32, 5::32, "field", 5::32, "value">>
    integers = <<0x88, 3::32, -1::signed-64, 0::signed-64, 9::signed-64>>

    cases = [
      {0x0202, record,
       %{
         "id" => "flow-1",
         "type" => "email",
         "state" => "queued",
         "attributes" => %{"tenant" => "acme"}
       }},
      {0x020E, record_list,
       [
         %{
           "id" => "flow-1",
           "type" => "email",
           "state" => "queued",
           "attributes" => %{"tenant" => "acme"}
         }
       ]},
      {0x000E, list_list, [["a", "bb"], []]},
      {0x000E, map_list, [%{"field" => "value"}]},
      {0x000E, integers, [-1, 0, 9]}
    ]

    Enum.each(cases, fn {opcode, payload, expected} ->
      body = <<0::16, payload::binary>>
      assert {:ok, ^expected} = Protocol.decode_response_body(0x02, opcode, body)
      assert {:ok, ^expected} = NativeCodec.decode_response(opcode, 0x02, body)
    end)

    structured_pipeline =
      IO.iodata_to_binary([
        <<0x95, 5::32, 0, 2>>,
        record,
        <<0, 5, 5::32, "ref-1", 0xFFFF_FFFF::32, 6::32, "flow-1", 0, 6, 2::32, 1::32, "a", 2::32,
          "bb", 0, 7, 1::32, 5::32, "field", 5::32, "value", 0, 4, 6::32, "flow-2",
          0xFFFF_FFFF::32, 7::32, "lease-1", 42::signed-64, 7::32, "running">>,
        Protocol.encode_value(%{"attempt" => 2})
      ])

    expected_pipeline = [
      ["ok", Enum.at(cases, 0) |> elem(2)],
      ["ok", %{"ref" => "ref-1", "partition_key" => nil, "owner_flow_id" => "flow-1"}],
      ["ok", ["a", "bb"]],
      ["ok", %{"field" => "value"}],
      ["ok", ["flow-2", nil, "lease-1", 42, "running", %{"attempt" => 2}]]
    ]

    body = <<0::16, structured_pipeline::binary>>
    assert {:ok, ^expected_pipeline} = Protocol.decode_response_body(0x02, 0x000E, body)
    assert {:ok, ^expected_pipeline} = NativeCodec.decode_response(0x000E, 0x02, body)
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

  test "compact Flow batches reject non-boolean independent values" do
    assert :error =
             Protocol.compact_flow_create_many_payload(%{
               "type" => "email",
               "state" => "queued",
               "now_ms" => 10,
               "run_at_ms" => 10,
               "independent" => "false",
               "items" => [["flow-1", ""]]
             })

    assert :error =
             Protocol.compact_flow_create_many_ids_payload(
               "email",
               "queued",
               nil,
               ["flow-1"],
               now_ms: 10,
               independent: "false"
             )

    assert :error =
             Protocol.compact_flow_complete_many_payload(%{
               "now_ms" => 10,
               "independent" => "false",
               "items" => [["flow-1", "lease-1", 10]]
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

  test "decodes current server state-only compact claim rows" do
    claim =
      IO.iodata_to_binary([
        <<6::32>>,
        "flow-1",
        <<0xFFFF_FFFF::32, 7::32>>,
        "lease-1",
        <<10::signed-64, 7::32>>,
        "running"
      ])

    direct_body = <<0::16, 0x80, 1::32, claim::binary>>

    assert {:ok, [["flow-1", nil, "lease-1", 10, "running"]]} =
             Protocol.decode_response_body(0, Protocol.opcode(:flow_claim_due), direct_body)

    pipeline_body = <<0::16, 0x95, 2::32, 0, 4, claim::binary, 0, 1, 2::32, "OK">>

    assert {:ok, [["ok", ["flow-1", nil, "lease-1", 10, "running"]], ["ok", "OK"]]} =
             Protocol.decode_response_body(0, Protocol.opcode(:pipeline), pipeline_body)
  end

  test "rejects compact collection counts before count-driven allocation" do
    excessive_count = 100_001

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(
               Protocol.opcode(:set),
               <<0x81, excessive_count::32>>
             )

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(
               Protocol.opcode(:pipeline),
               <<0x81, excessive_count::32>>
             )

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(
               Protocol.opcode(:mget),
               <<0x89, excessive_count::32, 0::32>>
             )
  end

  test "rejects oversized typed collection counts before recursive decoding" do
    assert {:error, :collection_too_large} = Protocol.decode_value(<<5, 100_001::32>>)
    assert {:error, :collection_too_large} = Protocol.decode_value(<<6, 100_001::32>>)
  end

  test "typed decoding enforces one total collection-item budget" do
    nested = <<5, 50_000::32, :binary.copy(<<0>>, 50_000)::binary>>
    payload = <<5, 3::32, nested::binary, nested::binary, nested::binary>>

    assert {:error, :collection_too_large} = Protocol.decode_value(payload)
  end

  test "compact records share the same total nested-value budget" do
    nested = <<5, 50_000::32, :binary.copy(<<0>>, 50_000)::binary>>
    payload = <<0x84, 2::32, 1, nested::binary, 2, nested::binary>>

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(Protocol.opcode(:flow_get), payload)
  end

  test "rejects oversized outbound collections before encoding every item" do
    values = List.duplicate(nil, 100_001)

    assert_raise ArgumentError, ~r/collection exceeds 100000 items/, fn ->
      Protocol.encode_value(values)
    end
  end

  test "enforces the server-compatible value nesting limit in both directions" do
    nested_value = Enum.reduce(1..64, "leaf", fn _level, value -> [value] end)
    nested_wire = [List.duplicate(<<5, 1::32>>, 64), Protocol.encode_value("leaf")]
    too_deep_wire = [<<5, 1::32>>, nested_wire]

    assert {:ok, ^nested_value, ""} =
             nested_wire |> IO.iodata_to_binary() |> Protocol.decode_value()

    assert {:error, :value_nesting_too_deep} =
             too_deep_wire |> IO.iodata_to_binary() |> Protocol.decode_value()

    assert byte_size(Protocol.encode_value(nested_value)) > 0

    too_deep_value = [nested_value]

    assert_raise ArgumentError, ~r/value nesting exceeds 64 levels/, fn ->
      Protocol.encode_value(too_deep_value)
    end
  end

  defp rejected_pipeline_reductions(argument_tail) do
    {:reductions, before_encoding} = Process.info(self(), :reductions)

    assert {:ok, payload} = Protocol.pipeline_payload_result([["ECHO", argument_tail]])

    assert {:error, :request_too_large} =
             RequestEncoder.encode(
               Protocol.opcode(:pipeline),
               1,
               1,
               payload,
               1_024
             )

    {:reductions, after_encoding} = Process.info(self(), :reductions)
    after_encoding - before_encoding
  end
end
