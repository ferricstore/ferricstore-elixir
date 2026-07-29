defmodule FerricStore.Protocol.FlowQueryRecordPerformanceTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias FerricStore.Protocol.{FlowQueryRecordDecoder, ValueCodec}

  test "decodes every negotiated record field without depending on insertion order" do
    fields = FlowQueryRecordDecoder.fields()
    values = Enum.to_list(0..(length(fields) - 1))

    encoded_values = values |> Enum.map(&ValueCodec.encode/1) |> IO.iodata_to_binary()
    bitmap = (1 <<< length(fields)) - 1

    assert {:ok, [record], <<>>} =
             FlowQueryRecordDecoder.decode_many(
               <<1::unsigned-32, bitmap::unsigned-32, encoded_values::binary>>
             )

    assert record == Map.new(Enum.zip(fields, values))
  end

  test "constructs each decoded record map once" do
    source =
      "../../../lib/ferric_store/protocol/flow_query_record_decoder.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    assert source =~ ":maps.from_list(fields)"
    refute source =~ "Map.put(record"
  end
end
