defmodule FerricStore.Flow.PolicyRetryValidator do
  @moduledoc false

  alias FerricStore.Flow.PolicyValidation

  @max_retries 1_000
  @max_delay_ms 2_592_000_000
  @max_retention_ttl_ms 31_536_000_000
  @max_history_events 1_000_000
  @backoff_kinds [:none, :fixed, :linear, :exponential, "none", "fixed", "linear", "exponential"]

  def validate(nil, _path), do: :ok

  def validate(value, path) do
    retry = PolicyValidation.option_map(value)

    with :ok <-
           PolicyValidation.bounded_integer(
             Map.fetch(retry, "max_retries"),
             0,
             @max_retries,
             path <> ".max_retries"
           ),
         :ok <- validate_backoff(Map.fetch(retry, "backoff"), path <> ".backoff") do
      validate_exhausted_to(Map.fetch(retry, "exhausted_to"), path <> ".exhausted_to")
    end
  end

  def validate_retention(nil, _path), do: :ok

  def validate_retention(value, path) do
    retention = PolicyValidation.option_map(value)

    with :ok <-
           PolicyValidation.bounded_integer(
             Map.fetch(retention, "ttl_ms"),
             1,
             @max_retention_ttl_ms,
             path <> ".ttl_ms"
           ) do
      PolicyValidation.bounded_integer(
        Map.fetch(retention, "history_max_events"),
        1,
        @max_history_events,
        path <> ".history_max_events"
      )
    end
  end

  defp validate_backoff(:error, _path), do: :ok

  defp validate_backoff({:ok, value}, path) when is_map(value) or is_list(value) do
    backoff = PolicyValidation.option_map(value)

    with :ok <-
           PolicyValidation.allowed(
             Map.fetch(backoff, "kind"),
             @backoff_kinds,
             path <> ".kind"
           ),
         :ok <- bounded_delay(backoff, "base_ms", path),
         :ok <- bounded_delay(backoff, "max_ms", path) do
      PolicyValidation.bounded_integer(
        Map.fetch(backoff, "jitter_pct"),
        0,
        100,
        path <> ".jitter_pct"
      )
    end
  end

  defp validate_backoff({:ok, _value}, path), do: PolicyValidation.error(path)

  defp bounded_delay(backoff, field, path) do
    PolicyValidation.bounded_integer(
      Map.fetch(backoff, field),
      0,
      @max_delay_ms,
      path <> "." <> field
    )
  end

  defp validate_exhausted_to(:error, _path), do: :ok

  defp validate_exhausted_to({:ok, value}, _path)
       when is_binary(value) and value != "" and value != "running",
       do: :ok

  defp validate_exhausted_to({:ok, _value}, path), do: PolicyValidation.error(path)
end
