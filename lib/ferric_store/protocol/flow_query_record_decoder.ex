defmodule FerricStore.Protocol.FlowQueryRecordDecoder do
  @moduledoc false

  import Bitwise

  alias FerricStore.FlowQueryLimits
  alias FerricStore.Protocol.{DecodeBudget, ValueCodec}

  @max_records FlowQueryLimits.max_records()
  @record_field_mask (1 <<< 20) - 1
  @record_fields {
    "id",
    "type",
    "state",
    "version",
    "priority",
    "partition_key",
    "created_at_ms",
    "updated_at_ms",
    "next_run_at_ms",
    "lease_deadline_ms",
    "attempts",
    "run_state",
    "max_active_ms",
    "parent_flow_id",
    "root_flow_id",
    "correlation_id",
    "attributes",
    "state_meta",
    "event_id",
    "fields"
  }

  def fields, do: Tuple.to_list(@record_fields)

  def decode_many(<<count::32, rest::binary>>) when count <= @max_records do
    with {:ok, budget} <- DecodeBudget.consume(DecodeBudget.new(), count) do
      take_records(count, rest, [], budget)
    end
  end

  def decode_many(<<_count::32, _rest::binary>>), do: {:error, :collection_too_large}
  def decode_many(_bytes), do: {:error, :truncated_compact_flow_query_records}

  defp take_records(0, rest, records, _budget), do: {:ok, Enum.reverse(records), rest}

  defp take_records(count, bytes, records, budget) do
    with {:ok, record, rest, budget} <- take_record(bytes, budget) do
      take_records(count - 1, rest, [record | records], budget)
    end
  end

  defp take_record(<<bitmap::32, rest::binary>>, budget)
       when band(bitmap, bnot(@record_field_mask)) == 0 do
    with {:ok, budget} <- DecodeBudget.consume(budget, population_count(bitmap)) do
      take_record_fields(0, bitmap, rest, [], budget)
    end
  end

  defp take_record(<<_bitmap::32, _rest::binary>>, _budget),
    do: {:error, :reserved_compact_flow_query_record_field}

  defp take_record(_bytes, _budget), do: {:error, :truncated_compact_flow_query_record}

  defp take_record_fields(20, _bitmap, rest, fields, budget),
    do: {:ok, :maps.from_list(fields), rest, budget}

  defp take_record_fields(index, bitmap, bytes, fields, budget) do
    if band(bitmap, 1 <<< index) == 0 do
      take_record_fields(index + 1, bitmap, bytes, fields, budget)
    else
      with {:ok, value, rest, budget} <- ValueCodec.decode_with_budget(bytes, budget) do
        take_record_fields(
          index + 1,
          bitmap,
          rest,
          [{elem(@record_fields, index), value} | fields],
          budget
        )
      end
    end
  end

  defp population_count(value), do: population_count(value, 0)
  defp population_count(0, count), do: count
  defp population_count(value, count), do: population_count(band(value, value - 1), count + 1)
end
