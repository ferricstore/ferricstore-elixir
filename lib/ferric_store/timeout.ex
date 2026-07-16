defmodule FerricStore.Timeout do
  @moduledoc false

  # Keep finite timers inside the portable Erlang timer domain. Callers that
  # genuinely need a longer lifetime can opt into :infinity explicitly.
  @max_finite 4_294_967_295

  @type t :: non_neg_integer() | :infinity

  @spec max_finite() :: pos_integer()
  def max_finite, do: @max_finite

  @spec valid?(term()) :: boolean()
  def valid?(:infinity), do: true
  def valid?(value), do: finite?(value)

  @spec finite?(term()) :: boolean()
  def finite?(value),
    do: is_integer(value) and value >= 0 and value <= @max_finite

  @spec positive?(term()) :: boolean()
  def positive?(:infinity), do: true

  def positive?(value),
    do: is_integer(value) and value > 0 and value <= @max_finite

  @spec add_margin(t(), non_neg_integer()) :: t()
  def add_margin(:infinity, _margin), do: :infinity

  def add_margin(value, margin)
      when is_integer(value) and value >= 0 and is_integer(margin) and margin >= 0 do
    min(value + margin, @max_finite)
  end
end
