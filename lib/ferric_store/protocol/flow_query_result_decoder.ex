defmodule FerricStore.Protocol.FlowQueryResultDecoder do
  @moduledoc false

  alias FerricStore.BinaryDetacher
  alias FerricStore.Protocol.FlowQueryRecordDecoder

  @contract "ferric.flow.query.result/v1"
  @max_integer 0x7FFF_FFFF_FFFF_FFFF
  @min_cursor_bytes 16
  @max_cursor_bytes 4_096
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
      record_fields: FlowQueryRecordDecoder.fields(),
      quality_fields: @quality_fields,
      usage_fields: @usage_fields
    }
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(<<0xA0, kind, rest::binary>> = payload) do
    with {:ok, quality, rest} <- take_quality(rest),
         {:ok, usage, rest} <- take_usage(rest),
         {:ok, shape, rest} <- take_shape(kind, rest, usage),
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

    usage = Map.new(Enum.zip(@usage_fields, values))

    if Enum.all?(values, &(&1 <= @max_integer)) and valid_usage?(usage) do
      {:ok, usage, rest}
    else
      {:error, :compact_flow_query_usage_out_of_range}
    end
  end

  defp take_usage(_bytes), do: {:error, :truncated_compact_flow_query_usage}

  defp take_shape(0, bytes, usage) do
    with {:ok, page, rest} <- take_page(bytes),
         {:ok, records, rest} <- FlowQueryRecordDecoder.decode_many(rest),
         true <- valid_record_usage?(usage, length(records)) do
      {:ok, %{"records" => records, "page" => page}, rest}
    else
      false -> {:error, :inconsistent_compact_flow_query_record_usage}
      {:error, _reason} = error -> error
    end
  end

  defp take_shape(1, <<count::unsigned-64, rest::binary>>, %{"result_records" => 1})
       when count <= @max_integer,
       do: {:ok, %{"result" => %{"kind" => "count", "value" => count}}, rest}

  defp take_shape(1, _bytes, _usage), do: {:error, :invalid_compact_flow_query_count}

  defp take_shape(_kind, _bytes, _usage),
    do: {:error, :unsupported_compact_flow_query_result_kind}

  defp take_page(<<0, 0xFFFF_FFFF::32, rest::binary>>),
    do: {:ok, %{"has_more" => false, "cursor" => nil}, rest}

  defp take_page(<<1, size::32, cursor::binary-size(size), rest::binary>>)
       when size >= @min_cursor_bytes and size <= @max_cursor_bytes do
    if String.valid?(cursor) and String.starts_with?(cursor, "fqc1_") do
      {:ok, %{"has_more" => true, "cursor" => BinaryDetacher.detach(cursor)}, rest}
    else
      {:error, :invalid_compact_flow_query_page}
    end
  end

  defp take_page(_bytes), do: {:error, :invalid_compact_flow_query_page}

  defp valid_usage?(usage) do
    usage["hydrated_records"] <= usage["scanned_entries"] and
      usage["duplicate_entries"] <= usage["scanned_entries"] and
      usage["range_pages"] <= usage["scanned_entries"] + usage["range_seeks"] and
      usage["residual_checks"] <= usage["scanned_entries"] * 12
  end

  defp valid_record_usage?(usage, count) do
    usage["result_records"] == count and
      usage["result_records"] <= usage["scanned_entries"]
  end
end
