defmodule FerricStore.DeadlineBudget do
  @moduledoc false

  @enforce_keys [:expires_at]
  defstruct [:expires_at]

  @type t :: %__MODULE__{expires_at: :infinity | integer()}

  @spec new(timeout()) :: t()
  def new(:infinity), do: %__MODULE__{expires_at: :infinity}

  def new(timeout) when is_integer(timeout) and timeout >= 0,
    do: %__MODULE__{expires_at: now_ms() + timeout}

  @spec remaining(t()) :: timeout()
  def remaining(%__MODULE__{expires_at: :infinity}), do: :infinity

  def remaining(%__MODULE__{expires_at: expires_at}),
    do: max(expires_at - now_ms(), 0)

  @spec request_timeout(t()) :: {:ok, timeout()} | {:error, :timeout}
  def request_timeout(%__MODULE__{} = budget) do
    case remaining(budget) do
      0 -> {:error, :timeout}
      timeout -> {:ok, timeout}
    end
  end

  @spec ensure_active(t()) :: :ok | {:error, :timeout}
  def ensure_active(%__MODULE__{} = budget) do
    case request_timeout(budget) do
      {:ok, _remaining} -> :ok
      {:error, :timeout} = error -> error
    end
  end

  @spec cap(t(), timeout()) :: timeout()
  def cap(%__MODULE__{} = budget, configured) do
    minimum(normalize(configured), remaining(budget))
  end

  @spec slice(t(), pos_integer()) :: t()
  def slice(%__MODULE__{expires_at: :infinity} = budget, candidate_count)
      when is_integer(candidate_count) and candidate_count > 0 do
    budget
  end

  def slice(%__MODULE__{expires_at: expires_at}, candidate_count)
      when is_integer(candidate_count) and candidate_count > 0 do
    now = now_ms()
    remaining = max(expires_at - now, 0)
    slice = div(remaining + candidate_count - 1, candidate_count)
    %__MODULE__{expires_at: min(expires_at, now + slice)}
  end

  defp minimum(:infinity, second), do: second
  defp minimum(first, :infinity), do: first
  defp minimum(first, second), do: min(first, second)

  defp normalize(:infinity), do: :infinity
  defp normalize(timeout) when is_integer(timeout) and timeout >= 0, do: timeout
  defp normalize(_invalid), do: 0

  defp now_ms, do: System.monotonic_time(:millisecond)
end
