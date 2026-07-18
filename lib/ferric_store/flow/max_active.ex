defmodule FerricStore.Flow.MaxActive do
  @moduledoc false

  @maximum_ms 31_536_000_000
  @infinity_values [nil, :infinity, "infinity", "INFINITY"]

  @spec valid?(term()) :: boolean()
  def valid?(value) when value in @infinity_values, do: true

  def valid?(value),
    do: is_integer(value) and value > 0 and value <= @maximum_ms
end
