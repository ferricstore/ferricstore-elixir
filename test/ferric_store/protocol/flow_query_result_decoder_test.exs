defmodule FerricStore.Protocol.FlowQueryResultDecoderTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{FlowQueryResultDecoder, Opcodes, ValueCodec}
  alias FerricStore.SDK.Native.Codec

  @context %{response_plan: nil, compact_codec: "flow_query_result_v1"}

  test "decodes the shared server golden corpus without schema drift" do
    corpus =
      Path.expand(
        "../../../test/fixtures/flow_query_result_v1.json",
        __DIR__
      )
      |> File.read!()
      |> Jason.decode!()

    schema = FlowQueryResultDecoder.schema()
    assert corpus["tag"] == schema.tag
    assert corpus["record_fields"] == schema.record_fields
    assert corpus["quality_fields"] == schema.quality_fields
    assert corpus["usage_fields"] == schema.usage_fields

    [page_vector, count_vector] = corpus["vectors"]

    assert {:ok, page} =
             page_vector["payload_hex"] |> decode_hex!() |> FlowQueryResultDecoder.decode()

    assert {:ok, count} =
             count_vector["payload_hex"] |> decode_hex!() |> FlowQueryResultDecoder.decode()

    assert page["records"] == [
             %{
               "id" => "run-1",
               "state" => "failed",
               "fields" => %{"invoice_total" => 42}
             }
           ]

    assert count["result"] == %{
             "kind" => "count",
             "value" => String.to_integer(count_vector["count_decimal"])
           }
  end

  test "decodes projected pages into the existing FQL1 result contract" do
    payload = page_payload()

    assert {:ok, result} =
             Codec.decode_response(
               Opcodes.flow_query(),
               0x02,
               <<0::16, payload::binary>>,
               byte_size(payload) + 2,
               @context
             )

    assert result["version"] == "ferric.flow.query.result/v1"
    assert result["page"] == %{"has_more" => false, "cursor" => nil}

    assert result["quality"] == %{
             "exactness" => "authoritative",
             "freshness" => "current",
             "coverage" => "complete",
             "pagination" => "authenticated_seek"
           }

    assert result["records"] == [
             %{
               "id" => "run-1",
               "state" => "failed",
               "fields" => %{"invoice_total" => 42}
             }
           ]

    assert result["usage"]["result_records"] == 1
    assert result["usage"]["response_bytes"] == byte_size(payload)
  end

  test "decodes count results at the signed 64-bit limit" do
    payload = count_payload(0x7FFF_FFFF_FFFF_FFFF)

    assert {:ok, %{"result" => %{"kind" => "count", "value" => 0x7FFF_FFFF_FFFF_FFFF}}} =
             Codec.decode_response(
               Opcodes.command_exec(),
               0x02,
               <<0::16, payload::binary>>,
               byte_size(payload) + 2,
               @context
             )
  end

  test "rejects reserved fields, truncation, trailing bytes, and unadvertised codecs" do
    payload = page_payload()
    <<prefix::binary-size(103), bitmap::32, rest::binary>> = payload
    reserved = <<prefix::binary, Bitwise.bor(bitmap, Bitwise.bsl(1, 20))::32, rest::binary>>

    malformed_payloads = [
      reserved,
      binary_part(payload, 0, byte_size(payload) - 1),
      payload <> <<0>>,
      cursor_page_payload("fqc1_short"),
      cursor_page_payload("other_cursor_token"),
      cursor_page_payload("fqc1_" <> :binary.copy(<<255>>, 11)),
      put_usage(payload, 0, 0x8000_0000_0000_0000),
      put_usage(payload, 4, 2),
      put_usage(payload, 7, 2),
      count_payload(42) |> put_usage(7, 0)
    ]

    for malformed <- malformed_payloads do
      assert {:error, _reason} =
               Codec.decode_response(
                 Opcodes.flow_query(),
                 0x02,
                 <<0::16, malformed::binary>>,
                 byte_size(malformed) + 2,
                 @context
               )
    end

    assert {:error, :unadvertised_compact_response} =
             Codec.decode_response(
               Opcodes.flow_query(),
               0x02,
               <<0::16, payload::binary>>,
               byte_size(payload) + 2,
               %{response_plan: nil, compact_codec: nil}
             )
  end

  test "binds the query-result tag to the explicitly selected response codec" do
    payload = page_payload()

    assert {:error, :compact_response_codec_mismatch} =
             Codec.decode_response(
               Opcodes.flow_query(),
               0x02,
               <<0::16, payload::binary>>,
               byte_size(payload) + 2,
               %{response_plan: nil, compact_codec: "kv_get_v1"}
             )

    assert {:error, :compact_response_codec_mismatch} =
             Codec.decode_response(
               Opcodes.flow_query(),
               0x02,
               <<0::16, 0x82, 0>>,
               4,
               @context
             )
  end

  defp page_payload do
    values =
      IO.iodata_to_binary([
        ValueCodec.encode("run-1"),
        ValueCodec.encode("failed"),
        ValueCodec.encode(%{"invoice_total" => 42})
      ])

    payload =
      <<0xA0, 0, 0, 0, 0, 2, usage(1)::binary, 0, 0xFFFF_FFFF::32, 1::32,
        Bitwise.bor(
          Bitwise.bor(Bitwise.bsl(1, 0), Bitwise.bsl(1, 2)),
          Bitwise.bsl(1, 19)
        )::32, values::binary>>

    put_response_bytes(payload)
  end

  defp count_payload(count) do
    put_response_bytes(<<0xA0, 1, 2, 1, 0, 0, usage(1)::binary, count::unsigned-64>>)
  end

  defp cursor_page_payload(cursor) do
    put_response_bytes(
      <<0xA0, 0, 0, 0, 0, 2, usage(0)::binary, 1, byte_size(cursor)::32, cursor::binary, 0::32>>
    )
  end

  defp usage(result_records) do
    <<0::64, 0::64, result_records::64, 0::64, result_records::64, 0::64, 0::64,
      result_records::64, 0::64, 0::64, 0::64>>
  end

  defp put_usage(payload, index, value) do
    offset = 6 + index * 8
    <<prefix::binary-size(^offset), _previous::64, rest::binary>> = payload
    <<prefix::binary, value::unsigned-64, rest::binary>>
  end

  defp put_response_bytes(payload) do
    <<prefix::binary-size(70), _previous::64, rest::binary>> = payload
    <<prefix::binary, byte_size(payload)::64, rest::binary>>
  end

  defp decode_hex!(value) do
    {:ok, payload} = Base.decode16(value, case: :mixed)
    payload
  end
end
