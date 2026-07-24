defmodule FerricStore.Protocol.FlowQueryResultDecoder do
  @moduledoc false

  import Bitwise

  alias FerricStore.BinaryDetacher
  alias FerricStore.Protocol.{DecodeBudget, ValueCodec}

  @contract "ferric.flow.query.result/v1"
  @max_integer 0x7FFF_FFFF_FFFF_FFFF
  @max_records 100
  @max_cursor_bytes 4_096
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
  @quality_fields ~w(exactness freshness coverage pagination)
  @quality_values [
    ~w(authoritative projected_exact exact not_applicable),
    ~w(current projection_watermark not_applicable),
    ~w(complete unavailable),
    ~w(none complete authenticated_seek live_seek)
  ]
  @usage_fields ~w(
    range_seeks range_pages scanned_entries scanned_bytes hydrated_records residual_checks
    duplicate_entries result_records response_bytes memory_high_water_bytes wall_time_us
  )

  @doc false
  def schema do
    %{
      tag: 0xA0,
      record_fields: Tuple.to_list(@record_fields),
      quality_fields: @quality_fields,
      usage_fields: @usage_fields
    }
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(<<0xA0, kind, rest::binary>> = payload) do
    with {:ok, quality, rest} <- take_quality(rest),
         {:ok, usage, rest} <- take_usage(rest),
         {:ok, shape, rest} <- take_shape(kind, rest),
         true <- rest == <<>> || {:error, :trailing_compact_flow_query_result_bytes},
         true <-
           usage["response_bytes"] == byte_size(payload) || {:error, :invalid_response_bytes} do
      {:ok,
       shape
       |> Map.put("version", @contract)
       |> Map.put("quality", quality)
       |> Map.put("usage", usage)}
    end
  end

  def decode(_payload), do: {:error, :invalid_compact_flow_query_result}

  defp take_quality(<<exactness, freshness, coverage, pagination, rest::binary>>) do
    codes = [exactness, freshness, coverage, pagination]

    values =
      @quality_values
      |> Enum.zip(codes)
      |> Enum.map(fn {options, code} -> Enum.at(options, code) end)

    if Enum.all?(values, &is_binary/1) do
      {:ok, Map.new(Enum.zip(@quality_fields, values)), rest}
    else
      {:error, :invalid_compact_flow_query_quality}
    end
  end

  defp take_quality(_bytes), do: {:error, :truncated_compact_flow_query_quality}

  defp take_usage(
         <<range_seeks::unsigned-64, range_pages::unsigned-64, scanned_entries::unsigned-64,
           scanned_bytes::unsigned-64, hydrated_records::unsigned-64,
           residual_checks::unsigned-64, duplicate_entries::unsigned-64,
           result_records::unsigned-64, response_bytes::unsigned-64,
           memory_high_water_bytes::unsigned-64, wall_time_us::unsigned-64, rest::binary>>
       ) do
    values = [
      range_seeks,
      range_pages,
      scanned_entries,
      scanned_bytes,
      hydrated_records,
      residual_checks,
      duplicate_entries,
      result_records,
      response_bytes,
      memory_high_water_bytes,
      wall_time_us
    ]

    if Enum.all?(values, &(&1 <= @max_integer)) do
      {:ok, Map.new(Enum.zip(@usage_fields, values)), rest}
    else
      {:error, :compact_flow_query_usage_out_of_range}
    end
  end

  defp take_usage(_bytes), do: {:error, :truncated_compact_flow_query_usage}

  defp take_shape(0, bytes) do
    with {:ok, page, rest} <- take_page(bytes),
         {:ok, records, rest} <- take_records(rest) do
      {:ok, %{"records" => records, "page" => page}, rest}
    end
  end

  defp take_shape(1, <<count::unsigned-64, rest::binary>>) when count <= @max_integer,
    do: {:ok, %{"result" => %{"kind" => "count", "value" => count}}, rest}

  defp take_shape(1, _bytes), do: {:error, :invalid_compact_flow_query_count}
  defp take_shape(_kind, _bytes), do: {:error, :unsupported_compact_flow_query_result_kind}

  defp take_page(<<0, 0xFFFF_FFFF::32, rest::binary>>),
    do: {:ok, %{"has_more" => false, "cursor" => nil}, rest}

  defp take_page(<<1, size::32, cursor::binary-size(size), rest::binary>>)
       when size > 0 and size <= @max_cursor_bytes,
       do: {:ok, %{"has_more" => true, "cursor" => BinaryDetacher.detach(cursor)}, rest}

  defp take_page(_bytes), do: {:error, :invalid_compact_flow_query_page}

  defp take_records(<<count::32, rest::binary>>) when count <= @max_records do
    with {:ok, budget} <- DecodeBudget.consume(DecodeBudget.new(), count) do
      take_records(count, rest, [], budget)
    end
  end

  defp take_records(<<_count::32, _rest::binary>>), do: {:error, :collection_too_large}
  defp take_records(_bytes), do: {:error, :truncated_compact_flow_query_records}

  defp take_records(0, rest, records, _budget), do: {:ok, Enum.reverse(records), rest}

  defp take_records(count, bytes, records, budget) do
    with {:ok, record, rest, budget} <- take_record(bytes, budget) do
      take_records(count - 1, rest, [record | records], budget)
    end
  end

  defp take_record(<<bitmap::32, rest::binary>>, budget)
       when band(bitmap, bnot(@record_field_mask)) == 0 do
    with {:ok, budget} <- DecodeBudget.consume(budget, population_count(bitmap)) do
      take_record_fields(0, bitmap, rest, %{}, budget)
    end
  end

  defp take_record(<<_bitmap::32, _rest::binary>>, _budget),
    do: {:error, :reserved_compact_flow_query_record_field}

  defp take_record(_bytes, _budget), do: {:error, :truncated_compact_flow_query_record}

  defp take_record_fields(20, _bitmap, rest, record, budget),
    do: {:ok, record, rest, budget}

  defp take_record_fields(index, bitmap, bytes, record, budget) do
    if band(bitmap, 1 <<< index) == 0 do
      take_record_fields(index + 1, bitmap, bytes, record, budget)
    else
      with {:ok, value, rest, budget} <- ValueCodec.decode_with_budget(bytes, budget) do
        take_record_fields(
          index + 1,
          bitmap,
          rest,
          Map.put(record, elem(@record_fields, index), value),
          budget
        )
      end
    end
  end

  defp population_count(value), do: population_count(value, 0)
  defp population_count(0, count), do: count
  defp population_count(value, count), do: population_count(band(value, value - 1), count + 1)
end
