defmodule FerricStore.Flow.QueryResponse.MetricsValidation do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Types

  @usage_fields [
    {"range_seeks", :range_seeks},
    {"range_pages", :range_pages},
    {"scanned_entries", :scanned_entries},
    {"scanned_bytes", :scanned_bytes},
    {"hydrated_records", :hydrated_records},
    {"residual_checks", :residual_checks},
    {"duplicate_entries", :duplicate_entries},
    {"result_records", :result_records},
    {"response_bytes", :response_bytes},
    {"memory_high_water_bytes", :memory_high_water_bytes},
    {"wall_time_us", :wall_time_us}
  ]
  @quality_values %{
    "exactness" => ~w(authoritative projected_exact exact not_applicable),
    "freshness" => ~w(current projection_watermark not_applicable),
    "coverage" => ~w(complete unavailable),
    "pagination" => ~w(none complete authenticated_seek live_seek)
  }

  def usage(value) when is_map(value) do
    with {:ok, usage} <- decode_usage_fields(value),
         true <- usage.hydrated_records <= usage.scanned_entries,
         true <- usage.duplicate_entries <= usage.scanned_entries,
         true <- usage.range_pages <= usage.scanned_entries + usage.range_seeks,
         true <- usage.residual_checks <= usage.scanned_entries * 12 do
      {:ok, usage}
    else
      false -> V.invalid(:usage_counters, value)
      {:error, _reason} = error -> error
    end
  end

  def usage(value), do: V.invalid(:usage, value)

  def quality(value) when is_map(value) do
    with {:ok, exactness} <- quality_value(value, "exactness"),
         {:ok, freshness} <- quality_value(value, "freshness"),
         {:ok, coverage} <- quality_value(value, "coverage"),
         {:ok, pagination} <- quality_value(value, "pagination") do
      {:ok,
       %{
         exactness: exactness,
         freshness: freshness,
         coverage: coverage,
         pagination: pagination
       }}
    end
  end

  def quality(value), do: V.invalid(:quality, value)

  defp decode_usage_fields(value) do
    Enum.reduce_while(@usage_fields, {:ok, %{}}, fn {field, atom}, {:ok, acc} ->
      case V.non_negative(value, field) do
        {:ok, number} -> {:cont, {:ok, Map.put(acc, atom, number)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp quality_value(value, field) do
    with {:ok, decoded} <- V.bounded_binary(value, field, 64),
         true <- decoded in Map.fetch!(@quality_values, field) do
      {:ok, decoded}
    else
      false -> V.invalid({:quality, field}, Types.get(value, field))
      {:error, _reason} = error -> error
    end
  end
end
