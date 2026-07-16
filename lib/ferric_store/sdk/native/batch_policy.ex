defmodule FerricStore.SDK.Native.BatchPolicy do
  @moduledoc false

  alias FerricStore.SDK.Native.RetryPolicy

  @type decision :: {:ok, [map()]} | {:retry, term()} | {:error, term()}

  @spec completion(map(), [map()], [map()]) :: decision()
  def completion(_batch, successes, []), do: {:ok, successes}

  def completion(batch, [], failures) do
    if retryable_failures?(batch, failures) do
      {:retry, retry_reason(failures)}
    else
      {:error, failure_reason(batch, failures)}
    end
  end

  def completion(_batch, successes, failures) do
    {:error, {:partial_group_failure, failure_details(successes, failures)}}
  end

  @spec sort_results([map()]) :: [map()]
  def sort_results(results) do
    Enum.sort_by(results, fn
      %{indexes: [index | _]} -> index
      _result -> -1
    end)
  end

  @spec group_failure(map(), term()) :: map()
  def group_failure(group, reason) do
    group
    |> Map.take([:route, :items, :payload, :indexes])
    |> Map.put(:reason, reason)
  end

  defp retryable_failures?(%{attempt: 0, opcode: opcode, opts: opts}, failures) do
    Enum.all?(failures, &RetryPolicy.retryable?(&1.reason, opcode, opts))
  end

  defp retryable_failures?(_batch, _failures), do: false

  defp retry_reason([failure]), do: failure.reason
  defp retry_reason(failures), do: {:group_failures, Enum.map(failures, & &1.reason)}

  defp failure_reason(%{attempt: attempt}, [failure]) when attempt > 0,
    do: failure.reason

  defp failure_reason(_batch, [failure]),
    do: {:group_failure, failure_details([], [failure])}

  defp failure_reason(_batch, failures),
    do: {:partial_group_failure, failure_details([], failures)}

  defp failure_details(successes, failures) do
    %{
      successes: Enum.map(successes, &Map.take(&1, [:indexes, :value])),
      failures: Enum.map(failures, &public_failure/1)
    }
  end

  defp public_failure(failure) do
    public = Map.take(failure, [:indexes, :reason])

    case Map.get(failure, :route) do
      route when is_map(route) -> Map.put(public, :route, Map.take(route, [:shard, :lane_id]))
      _missing -> public
    end
  end
end
