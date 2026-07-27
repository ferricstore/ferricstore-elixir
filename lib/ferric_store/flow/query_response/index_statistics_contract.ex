defmodule FerricStore.Flow.QueryResponse.IndexStatisticsContract do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V

  def validate(statistics, status) do
    timestamps = [
      statistics.oldest_collected_at_ms,
      statistics.newest_collected_at_ms,
      statistics.oldest_age_ms,
      statistics.newest_age_ms
    ]

    if statistics.samples == 0 do
      validate_empty(statistics, timestamps)
    else
      validate_sampled(statistics, status, timestamps)
    end
  end

  def validate_service(status) do
    statuses = Enum.map(status.indexes, & &1.statistics.status)

    valid =
      if status.services.statistics_store == "unavailable",
        do: Enum.all?(statuses, &(&1 == "unavailable")),
        else: Enum.all?(statuses, &(&1 != "unavailable"))

    if valid, do: :ok, else: fail(status, :statistics_service)
  end

  defp validate_empty(statistics, timestamps) do
    valid =
      statistics.status in ~w(missing unavailable) and Enum.all?(timestamps, &is_nil/1)

    if valid, do: :ok, else: V.invalid(:index_statistics, statistics.raw)
  end

  defp validate_sampled(statistics, status, timestamps) do
    if Enum.any?(timestamps, &is_nil/1) do
      V.invalid(:index_statistics, statistics.raw)
    else
      validate_sampled_values(statistics, status, timestamps)
    end
  end

  defp validate_sampled_values(statistics, status, [
         oldest,
         newest,
         oldest_age,
         newest_age
       ]) do
    valid =
      oldest <= newest and oldest_age == max(status.observed_at_ms - oldest, 0) and
        newest_age == max(status.observed_at_ms - newest, 0) and
        statistics.status in expected_statuses(statistics)

    if valid, do: :ok, else: V.invalid(:index_statistics, statistics.raw)
  end

  defp expected_statuses(%{fresh_samples: samples, samples: samples}), do: ["fresh"]

  defp expected_statuses(%{fresh_samples: 0, future_samples: future_samples})
       when future_samples > 0,
       do: ~w(stale future)

  defp expected_statuses(%{fresh_samples: 0}), do: ["stale"]
  defp expected_statuses(_statistics), do: ["mixed"]

  defp fail(status, reason), do: V.invalid({:index_contract, reason}, status.raw)
end
