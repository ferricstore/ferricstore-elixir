defmodule FerricStore.Flow.Options.CrossValueValidator do
  @moduledoc false

  @max_exact 9_007_199_254_740_991

  @spec validate(atom(), keyword()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_ranges(operation, opts),
         :ok <- validate_history_caps(operation, opts),
         :ok <- validate_running_transition(operation, opts),
         :ok <- validate_named_value_ttl(operation, opts),
         do: validate_deadline(operation, opts)
  end

  defp validate_ranges(operation, opts) when operation in [:history, :list, :search] do
    with :ok <- ordered(operation, opts, :from_ms, :to_ms),
         :ok <- ordered(operation, opts, :from_version, :to_version),
         do: ordered_events(operation, opts)
  end

  defp validate_ranges(_operation, _opts), do: :ok

  defp ordered(operation, opts, from, to) do
    case {Keyword.fetch(opts, from), Keyword.fetch(opts, to)} do
      {{:ok, first}, {:ok, last}} when first > last ->
        invalid(operation, from, {:must_not_exceed, to})

      _missing_or_ordered ->
        :ok
    end
  end

  defp ordered_events(:history, opts) do
    case {Keyword.fetch(opts, :from_event), Keyword.fetch(opts, :to_event)} do
      {{:ok, first}, {:ok, last}} ->
        if event_key(first) <= event_key(last),
          do: :ok,
          else: invalid(:history, :from_event, {:must_not_exceed, :to_event})

      _missing ->
        :ok
    end
  end

  defp ordered_events(_operation, _opts), do: :ok

  defp event_key(value) do
    [milliseconds, version] = :binary.split(value, "-")
    {String.to_integer(milliseconds), String.to_integer(version)}
  end

  defp validate_history_caps(:create, opts) do
    case {Keyword.fetch(opts, :history_hot_max_events), Keyword.fetch(opts, :history_max_events)} do
      {{:ok, hot}, {:ok, total}} when hot > total ->
        invalid(:create, :history_hot_max_events, {:must_not_exceed, :history_max_events})

      _missing_or_ordered ->
        :ok
    end
  end

  defp validate_history_caps(_operation, _opts), do: :ok

  defp validate_running_transition(:transition, opts),
    do: reject_running(:transition, :to_state, Keyword.get(opts, :to_state))

  defp validate_running_transition(:signal, opts),
    do: reject_running(:signal, :transition_to, Keyword.get(opts, :transition_to))

  defp validate_running_transition(_operation, _opts), do: :ok

  defp reject_running(operation, option, "running"),
    do: invalid(operation, option, :reserved_running_state)

  defp reject_running(_operation, _option, _value), do: :ok

  defp validate_named_value_ttl(:value_put, opts) do
    if Keyword.has_key?(opts, :ttl_ms) and is_binary(Keyword.get(opts, :owner_flow_id)) and
         is_binary(Keyword.get(opts, :name)),
       do: invalid(:value_put, :ttl_ms, {:conflicts_with, [:name, :owner_flow_id]}),
       else: :ok
  end

  defp validate_named_value_ttl(_operation, _opts), do: :ok

  defp validate_deadline(:claim_due, opts),
    do:
      deadline(
        :claim_due,
        :lease_ms,
        Keyword.get(opts, :now_ms),
        Keyword.get(opts, :lease_ms, 30_000)
      )

  defp validate_deadline(operation, opts)
       when operation in [:cancel, :complete, :complete_many, :fail, :value_put],
       do: deadline(operation, :ttl_ms, Keyword.get(opts, :now_ms), Keyword.get(opts, :ttl_ms))

  defp validate_deadline(_operation, _opts), do: :ok

  defp deadline(operation, option, now, duration)
       when is_integer(now) and is_integer(duration) and now > @max_exact - duration,
       do: invalid(operation, option, {:deadline_exceeds, @max_exact})

  defp deadline(_operation, _option, _now, _duration), do: :ok

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
