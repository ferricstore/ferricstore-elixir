defmodule FerricStore.Flow.Options.HistoryCursorValidator do
  @moduledoc false

  @max_exact 9_007_199_254_740_991

  def validate(:history, opts) do
    with :ok <- validate_option(opts, :from_event),
         do: validate_option(opts, :to_event)
  end

  def validate(_operation, _opts), do: :ok

  defp validate_option(opts, option) do
    case Keyword.fetch(opts, option) do
      :error -> :ok
      {:ok, value} -> if valid?(value), do: :ok, else: invalid(option)
    end
  end

  defp valid?(nil), do: true

  defp valid?(value) when is_binary(value) and byte_size(value) <= 33 do
    case :binary.split(value, "-") do
      [milliseconds, version] -> canonical_exact?(milliseconds) and canonical_exact?(version)
      _invalid -> false
    end
  end

  defp valid?(_value), do: false

  defp canonical_exact?(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 and number <= @max_exact -> value == Integer.to_string(number)
      _invalid -> false
    end
  end

  defp invalid(option),
    do: {:error, {:invalid_flow_option, :history, option, :expected_history_event_id}}
end
