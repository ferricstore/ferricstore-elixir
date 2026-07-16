defmodule FerricStore.SDK.Native.AdmissionGate do
  @moduledoc false

  @enforce_keys [:counter, :limit]
  defstruct [:counter, :limit]

  @type t :: %__MODULE__{counter: :atomics.atomics_ref(), limit: pos_integer()}

  @spec new(pos_integer()) :: t()
  def new(limit) when is_integer(limit) and limit > 0,
    do: %__MODULE__{counter: :atomics.new(1, signed: false), limit: limit}

  @spec acquire(t()) :: :ok | {:error, :client_backpressure}
  def acquire(%__MODULE__{counter: counter, limit: limit}) do
    if :atomics.add_get(counter, 1, 1) <= limit do
      :ok
    else
      _remaining = :atomics.sub_get(counter, 1, 1)
      {:error, :client_backpressure}
    end
  end

  @spec release(t()) :: :ok
  def release(%__MODULE__{counter: counter}) do
    saturating_decrement(counter)
    :ok
  end

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{counter: counter}), do: :atomics.get(counter, 1)

  defp saturating_decrement(counter) do
    case :atomics.get(counter, 1) do
      0 ->
        :ok

      current ->
        case :atomics.compare_exchange(counter, 1, current, current - 1) do
          :ok -> :ok
          _changed -> saturating_decrement(counter)
        end
    end
  end
end
