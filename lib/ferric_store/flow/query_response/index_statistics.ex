defmodule FerricStore.Flow.QueryResponse.IndexStatistics do
  @moduledoc false

  alias FerricStore.Flow.QueryIndexStatistics
  alias FerricStore.Flow.QueryResponse.Validation
  alias FerricStore.Types

  def decode(value) when is_map(value) do
    with {:ok, status} <-
           choice(
             value,
             "status",
             ~w(fresh stale future mixed missing unavailable),
             :statistics_status
           ),
         {:ok, samples} <- Validation.unsigned(value, "samples"),
         {:ok, fresh_samples} <- Validation.unsigned(value, "fresh_samples"),
         {:ok, stale_samples} <- Validation.unsigned(value, "stale_samples"),
         {:ok, future_samples} <- Validation.unsigned(value, "future_samples"),
         true <- fresh_samples + stale_samples == samples and future_samples <= stale_samples,
         {:ok, oldest_collected_at_ms} <- nullable_unsigned(value, "oldest_collected_at_ms"),
         {:ok, newest_collected_at_ms} <- nullable_unsigned(value, "newest_collected_at_ms"),
         {:ok, oldest_age_ms} <- nullable_unsigned(value, "oldest_age_ms"),
         {:ok, newest_age_ms} <- nullable_unsigned(value, "newest_age_ms") do
      {:ok,
       %QueryIndexStatistics{
         status: status,
         samples: samples,
         fresh_samples: fresh_samples,
         stale_samples: stale_samples,
         future_samples: future_samples,
         oldest_collected_at_ms: oldest_collected_at_ms,
         newest_collected_at_ms: newest_collected_at_ms,
         oldest_age_ms: oldest_age_ms,
         newest_age_ms: newest_age_ms,
         raw: value
       }}
    else
      false -> Validation.invalid(:statistics, value)
      {:error, _reason} = error -> error
    end
  end

  def decode(value), do: Validation.invalid(:statistics, value)

  defp nullable_unsigned(value, field) do
    if Validation.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> Validation.unsigned(value, field)
      end
    else
      Validation.invalid({:nullable, field}, value)
    end
  end

  defp choice(value, field, choices, error_field) do
    actual = Types.get(value, field)
    if actual in choices, do: {:ok, actual}, else: Validation.invalid(error_field, actual)
  end
end
