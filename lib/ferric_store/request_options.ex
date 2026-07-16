defmodule FerricStore.RequestOptions do
  @moduledoc false

  alias FerricStore.{OptionList, RequestLimits, Timeout}

  @max_options 32
  @explicit_call_cleanup_margin 20
  @request_reply_margin 1_000
  @timeout_keys [:timeout, :call_timeout]
  @typed_options [:idempotent, :lane_id, :max_group_concurrency, :request_context]

  @spec validate(keyword()) :: :ok | {:error, {atom(), term()}}
  def validate(opts) when is_list(opts) do
    with :ok <- OptionList.validate(opts, @max_options),
         :ok <- Enum.reduce_while(@timeout_keys, :ok, &validate_timeout_option(opts, &1, &2)) do
      validate_typed_options(opts)
    end
  end

  def validate(opts), do: {:error, {:options, opts}}

  @spec validate_supported(term(), [atom()]) :: :ok | {:error, {atom(), term()}}
  def validate_supported(opts, supported) when is_list(supported) do
    with :ok <- validate(opts) do
      unsupported_option(opts, supported)
    end
  end

  defp unsupported_option(opts, supported) do
    case Enum.find(opts, fn {key, _value} -> key not in supported end) do
      nil -> :ok
      {key, value} -> {:error, {key, value}}
    end
  end

  defp validate_typed_options(opts) do
    Enum.reduce_while(@typed_options, :ok, fn key, :ok ->
      case Keyword.fetch(opts, key) do
        :error ->
          {:cont, :ok}

        {:ok, value} ->
          validate_typed_option(key, value)
      end
    end)
  end

  defp validate_typed_option(key, value) do
    if valid_typed_option?(key, value),
      do: {:cont, :ok},
      else: {:halt, {:error, {key, value}}}
  end

  defp valid_typed_option?(:idempotent, value), do: is_boolean(value)
  defp valid_typed_option?(:lane_id, value), do: is_integer(value) and value in 0..0xFFFF_FFFF

  defp valid_typed_option?(:max_group_concurrency, value),
    do: is_integer(value) and value > 0 and value <= RequestLimits.max_group_concurrency()

  defp valid_typed_option?(:request_context, value), do: is_map(value)

  defp validate_timeout_option(opts, key, :ok) do
    case Keyword.fetch(opts, key) do
      :error -> {:cont, :ok}
      {:ok, value} -> timeout_validation_result(key, value)
    end
  end

  defp timeout_validation_result(key, value) do
    if Timeout.valid?(value),
      do: {:cont, :ok},
      else: {:halt, {:error, {key, value}}}
  end

  @spec pending_timeout(keyword(), timeout()) :: timeout()
  def pending_timeout(opts, default_timeout) do
    minimum_budget(request_timeout_budget(opts, default_timeout), call_timeout_budget(opts))
  end

  @spec call_timeout(keyword(), timeout(), timeout()) :: timeout()
  def call_timeout(_opts, _default_timeout, 0), do: 0

  def call_timeout(opts, default_timeout, remaining) do
    configured = configured_call_timeout(opts, default_timeout)
    margin = call_timeout_margin(opts)
    bounded_call_timeout(configured, remaining, margin)
  end

  defp bounded_call_timeout(:infinity, _remaining, _margin), do: :infinity

  defp bounded_call_timeout(configured, remaining, margin),
    do: min(configured, add_margin(remaining, margin))

  defp configured_call_timeout(opts, default_timeout) do
    case Keyword.get(opts, :call_timeout) do
      nil ->
        add_call_timeout_margin(Keyword.get(opts, :timeout, default_timeout), default_timeout)

      timeout ->
        timeout
    end
  end

  defp call_timeout_margin(opts) do
    case Keyword.fetch(opts, :call_timeout) do
      :error ->
        @request_reply_margin

      {:ok, timeout} when is_integer(timeout) and timeout > @explicit_call_cleanup_margin ->
        @explicit_call_cleanup_margin

      {:ok, _timeout} ->
        0
    end
  end

  defp add_margin(:infinity, _margin), do: :infinity
  defp add_margin(timeout, margin), do: Timeout.add_margin(timeout, margin)

  defp request_timeout_budget(opts, default_timeout) do
    case Keyword.get(opts, :timeout, default_timeout) do
      :infinity -> :infinity
      timeout when is_integer(timeout) and timeout >= 0 -> timeout
      _other -> default_timeout
    end
  end

  defp call_timeout_budget(opts) do
    case Keyword.get(opts, :call_timeout) do
      :infinity ->
        :infinity

      timeout when is_integer(timeout) and timeout > @explicit_call_cleanup_margin ->
        timeout - @explicit_call_cleanup_margin

      timeout when is_integer(timeout) and timeout >= 0 ->
        timeout

      _other ->
        :infinity
    end
  end

  defp minimum_budget(:infinity, second), do: second
  defp minimum_budget(first, :infinity), do: first
  defp minimum_budget(first, second), do: min(first, second)

  defp add_call_timeout_margin(:infinity, _default_timeout), do: :infinity

  defp add_call_timeout_margin(timeout, _default_timeout)
       when is_integer(timeout) and timeout >= 0,
       do: Timeout.add_margin(timeout, @request_reply_margin)

  defp add_call_timeout_margin(_timeout, default_timeout),
    do: default_timeout + @request_reply_margin
end
