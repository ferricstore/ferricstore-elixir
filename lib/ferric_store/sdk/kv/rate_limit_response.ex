defmodule FerricStore.SDK.KV.RateLimitResponse do
  @moduledoc false

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  @spec validate(term(), pos_integer(), pos_integer(), pos_integer()) :: :ok | :error
  def validate([status, count, remaining, reset_ms], maximum, window_ms, increment) do
    with :ok <- state(status, count, remaining, maximum, increment),
         true <- is_integer(reset_ms) and reset_ms >= 0 and reset_ms <= window_ms do
      :ok
    else
      _invalid -> :error
    end
  end

  def validate(_value, _maximum, _window_ms, _increment), do: :error

  defp state("allowed", count, remaining, maximum, increment)
       when is_signed_64_integer(count) and count >= increment and count <= maximum and
              remaining == maximum - count,
       do: :ok

  defp state("denied", count, 0, maximum, increment)
       when is_signed_64_integer(count) and count >= maximum and count + increment > maximum,
       do: :ok

  defp state("denied", count, remaining, maximum, increment)
       when is_signed_64_integer(count) and count >= 0 and count < maximum and
              remaining == maximum - count and count + increment > maximum,
       do: :ok

  defp state(_status, _count, _remaining, _maximum, _increment), do: :error
end
