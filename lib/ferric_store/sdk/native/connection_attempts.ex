defmodule FerricStore.SDK.Native.ConnectionAttempts do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionAttemptBatchIndex

  defstruct by_key: %{}, waiters_by_batch: %{}

  @type key :: term()
  @type waiter :: term()
  @type attempt :: %{required(:waiters) => MapSet.t(waiter())}
  @type t :: %__MODULE__{
          by_key: %{optional(key()) => attempt()},
          waiters_by_batch: %{
            optional(reference()) => %{optional(key()) => MapSet.t(waiter())}
          }
        }

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{by_key: by_key}), do: map_size(by_key)

  @spec contains?(t(), key()) :: boolean()
  def contains?(%__MODULE__{by_key: by_key}, key), do: Map.has_key?(by_key, key)

  @spec fetch(t(), key()) :: attempt() | nil
  def fetch(%__MODULE__{by_key: by_key}, key), do: Map.get(by_key, key)

  @spec fetch!(t(), key()) :: attempt()
  def fetch!(%__MODULE__{by_key: by_key}, key), do: Map.fetch!(by_key, key)

  @spec put(t(), key(), attempt()) :: t()
  def put(%__MODULE__{} = attempts, key, attempt) do
    {_previous, attempts} = pop(attempts, key)

    %{
      attempts
      | by_key: Map.put(attempts.by_key, key, attempt),
        waiters_by_batch:
          Enum.reduce(attempt.waiters, attempts.waiters_by_batch, fn waiter, index ->
            ConnectionAttemptBatchIndex.put(index, key, waiter)
          end)
    }
  end

  @spec pop(t(), key()) :: {attempt() | nil, t()}
  def pop(%__MODULE__{} = attempts, key) do
    case Map.pop(attempts.by_key, key) do
      {nil, _by_key} ->
        {nil, attempts}

      {attempt, by_key} ->
        attempts = %{
          attempts
          | by_key: by_key,
            waiters_by_batch:
              Enum.reduce(attempt.waiters, attempts.waiters_by_batch, fn waiter, index ->
                ConnectionAttemptBatchIndex.delete(index, key, waiter)
              end)
        }

        {attempt, attempts}
    end
  end

  @spec add_waiter(t(), key(), waiter()) :: t()
  def add_waiter(%__MODULE__{} = attempts, key, waiter) do
    case Map.fetch(attempts.by_key, key) do
      {:ok, attempt} ->
        if MapSet.member?(attempt.waiters, waiter) do
          attempts
        else
          attempt = %{attempt | waiters: MapSet.put(attempt.waiters, waiter)}

          %{
            attempts
            | by_key: Map.put(attempts.by_key, key, attempt),
              waiters_by_batch:
                ConnectionAttemptBatchIndex.put(attempts.waiters_by_batch, key, waiter)
          }
        end

      :error ->
        attempts
    end
  end

  @spec remove_waiter(t(), key(), waiter()) ::
          {:missing, t()} | {:remaining, t()} | {:empty, attempt(), t()}
  def remove_waiter(%__MODULE__{} = attempts, key, waiter) do
    case Map.fetch(attempts.by_key, key) do
      :error ->
        {:missing, attempts}

      {:ok, attempt} ->
        remove_existing_waiter(attempts, key, waiter, attempt)
    end
  end

  defp remove_existing_waiter(attempts, key, waiter, attempt) do
    if MapSet.member?(attempt.waiters, waiter) do
      remove_indexed_waiter(attempts, key, waiter, attempt)
    else
      {:missing, attempts}
    end
  end

  defp remove_indexed_waiter(attempts, key, waiter, attempt) do
    waiters = MapSet.delete(attempt.waiters, waiter)

    case MapSet.size(waiters) do
      0 -> remove_empty_attempt(attempts, key, waiter, attempt)
      _remaining -> update_attempt_waiters(attempts, key, waiter, attempt, waiters)
    end
  end

  defp remove_empty_attempt(attempts, key, waiter, attempt) do
    attempts = %{
      attempts
      | by_key: Map.delete(attempts.by_key, key),
        waiters_by_batch:
          ConnectionAttemptBatchIndex.delete(attempts.waiters_by_batch, key, waiter)
    }

    {:empty, attempt, attempts}
  end

  defp update_attempt_waiters(attempts, key, waiter, attempt, waiters) do
    attempt = %{attempt | waiters: waiters}

    attempts = %{
      attempts
      | by_key: Map.put(attempts.by_key, key, attempt),
        waiters_by_batch:
          ConnectionAttemptBatchIndex.delete(attempts.waiters_by_batch, key, waiter)
    }

    {:remaining, attempts}
  end

  @spec remove_batch_waiters(t(), reference()) :: {[{key(), attempt()}], t()}
  def remove_batch_waiters(%__MODULE__{} = attempts, batch_id) when is_reference(batch_id) do
    {waiters_by_key, waiters_by_batch} =
      Map.pop(attempts.waiters_by_batch, batch_id, %{})

    attempts = %{attempts | waiters_by_batch: waiters_by_batch}

    {emptied, attempts} =
      Enum.reduce(waiters_by_key, {[], attempts}, fn {key, batch_waiters}, {emptied, attempts} ->
        remove_indexed_batch_waiters(attempts, key, batch_waiters, emptied)
      end)

    {Enum.reverse(emptied), attempts}
  end

  defp remove_indexed_batch_waiters(attempts, key, batch_waiters, emptied) do
    case Map.fetch(attempts.by_key, key) do
      :error ->
        {emptied, attempts}

      {:ok, attempt} ->
        waiters = Enum.reduce(batch_waiters, attempt.waiters, &MapSet.delete(&2, &1))

        if MapSet.size(waiters) == 0 do
          attempts = %{
            attempts
            | by_key: Map.delete(attempts.by_key, key)
          }

          {[{key, attempt} | emptied], attempts}
        else
          attempt = %{attempt | waiters: waiters}
          {emptied, %{attempts | by_key: Map.put(attempts.by_key, key, attempt)}}
        end
    end
  end
end
