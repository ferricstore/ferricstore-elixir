defmodule FerricStore.Flow.Options.NumericValueValidator do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  @max_exact 9_007_199_254_740_991
  @max_active_ms 31_536_000_000
  @max_history_hot_events 10_000
  @max_history_events 1_000_000

  @nonnegative_exact %{
    from_ms: [:history, :list, :search],
    from_version: [:history],
    max_bytes: [:value_mget],
    now_ms: [
      :cancel,
      :claim_due,
      :complete,
      :complete_many,
      :create,
      :create_many,
      :fail,
      :retry,
      :signal,
      :transition,
      :value_put
    ],
    payload_max_bytes: [:claim_due, :get, :history, :value_mget],
    run_at_ms: [:create, :create_many, :retry, :signal, :transition],
    to_ms: [:history, :list, :search],
    to_version: [:history],
    value_max_bytes: [:claim_due, :value_mget]
  }
  @positive_exact %{
    count: [:history, :list, :search],
    lease_ms: [:claim_due],
    limit: [:claim_due],
    ttl_ms: [:cancel, :complete, :complete_many, :fail, :value_put]
  }
  @positive_signed %{retention_ttl_ms: [:create, :create_many]}
  @special %{
    block_ms: {[:claim_due], :unsigned_32},
    history_hot_max_events: {[:create], :history_hot},
    history_max_events: {[:create], :history},
    max_active_ms: {[:create], :max_active},
    priority: {[:claim_due, :create, :create_many, :transition], :priority},
    reclaim_ratio: {[:claim_due], :percentage}
  }

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_options(operation, opts, @nonnegative_exact, :nonnegative_exact),
         :ok <- validate_options(operation, opts, @positive_exact, :positive_exact),
         :ok <- validate_options(operation, opts, @positive_signed, :positive_signed),
         do: validate_special(operation, opts)
  end

  defp validate_options(operation, opts, specs, domain) do
    Enum.reduce_while(specs, :ok, fn {option, operations}, :ok ->
      validate_option(operation, opts, option, operations, domain)
    end)
  end

  defp validate_special(operation, opts) do
    Enum.reduce_while(@special, :ok, fn {option, {operations, domain}}, :ok ->
      validate_option(operation, opts, option, operations, domain)
    end)
  end

  defp validate_option(operation, opts, option, operations, domain) do
    if operation in operations do
      case Keyword.fetch(opts, option) do
        :error -> {:cont, :ok}
        {:ok, value} -> value_result(value, operation, option, domain)
      end
    else
      {:cont, :ok}
    end
  end

  defp value_result(value, operation, option, domain) do
    if valid?(domain, value),
      do: {:cont, :ok},
      else: {:halt, {:error, {:invalid_flow_option, operation, option, expectation(domain)}}}
  end

  defp valid?(:nonnegative_exact, value),
    do: is_integer(value) and value >= 0 and value <= @max_exact

  defp valid?(:positive_exact, value),
    do: is_integer(value) and value > 0 and value <= @max_exact

  defp valid?(:positive_signed, value), do: is_signed_64_integer(value) and value > 0

  defp valid?(:unsigned_32, value),
    do: is_integer(value) and value >= 0 and value <= 0xFFFFFFFF

  defp valid?(:priority, value), do: is_integer(value) and value in 0..2
  defp valid?(:percentage, value), do: is_integer(value) and value in 0..100

  defp valid?(:max_active, value),
    do:
      value in [nil, :infinity, "infinity", "INFINITY"] or
        (is_integer(value) and value > 0 and value <= @max_active_ms)

  defp valid?(:history_hot, value),
    do: is_integer(value) and value >= 0 and value <= @max_history_hot_events

  defp valid?(:history, value),
    do: is_integer(value) and value > 0 and value <= @max_history_events

  defp expectation(:nonnegative_exact), do: :expected_nonnegative_exact_integer
  defp expectation(:positive_exact), do: :expected_positive_exact_integer
  defp expectation(:positive_signed), do: :expected_positive_signed_64_integer
  defp expectation(:unsigned_32), do: :expected_unsigned_32_integer
  defp expectation(:priority), do: :expected_priority
  defp expectation(:percentage), do: :expected_percentage_integer
  defp expectation(:max_active), do: :expected_positive_bounded_duration_or_infinity
  defp expectation(:history_hot), do: :expected_history_hot_event_limit
  defp expectation(:history), do: :expected_history_event_limit
end
