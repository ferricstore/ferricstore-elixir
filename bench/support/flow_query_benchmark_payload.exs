defmodule FerricStore.FlowQueryBenchmarkPayload do
  @moduledoc false

  import Bitwise

  alias FerricStore.Protocol.ValueCodec

  @full_field_indexes Enum.to_list(0..17)
  @projected_field_indexes [0, 2, 16]

  def page(record_count, :full), do: page_payload(record_count, @full_field_indexes)
  def page(record_count, :projected), do: page_payload(record_count, @projected_field_indexes)
  def count(count), do: response_payload(1, 1, <<count::unsigned-64>>)
  def field_count(:full), do: length(@full_field_indexes)
  def field_count(:projected), do: length(@projected_field_indexes)

  defp page_payload(record_count, field_indexes) do
    records =
      Enum.map(1..record_count, fn index ->
        values = record_values(index)
        bitmap = Enum.reduce(field_indexes, 0, &bor(&2, 1 <<< &1))
        encoded = Enum.map(field_indexes, &ValueCodec.encode(Enum.at(values, &1)))
        [<<bitmap::unsigned-32>>, encoded]
      end)

    shape = [<<0, 0xFFFF_FFFF::unsigned-32, record_count::unsigned-32>>, records]
    response_payload(0, record_count, shape)
  end

  defp response_payload(kind, result_records, shape) do
    usage =
      <<1::unsigned-64, 1::unsigned-64, result_records::unsigned-64,
        result_records * 128::unsigned-64, result_records::unsigned-64, 0::unsigned-64,
        0::unsigned-64, result_records::unsigned-64, 0::unsigned-64, 0::unsigned-64,
        0::unsigned-64>>

    payload = IO.iodata_to_binary([<<0xA0, kind, 0, 0, 0, 2>>, usage, shape])
    put_response_bytes(payload)
  end

  defp put_response_bytes(payload) do
    <<prefix::binary-size(70), _response_bytes::unsigned-64, rest::binary>> = payload
    <<prefix::binary, byte_size(payload)::unsigned-64, rest::binary>>
  end

  defp record_values(index) do
    [
      "run-#{index}",
      "invoice",
      "queued",
      index,
      0,
      "tenant-#{rem(index, 10)}",
      1_000 + index,
      2_000 + index,
      3_000 + index,
      4_000 + index,
      rem(index, 5),
      "active",
      60_000,
      "parent-#{index}",
      "root-#{index}",
      "correlation-#{index}",
      %{"customer" => "customer-#{index}", "region" => "eu"},
      %{"queued" => %{"worker" => "worker-#{rem(index, 8)}"}},
      "event-#{index}",
      %{"worker" => "worker-#{index}"}
    ]
  end
end
