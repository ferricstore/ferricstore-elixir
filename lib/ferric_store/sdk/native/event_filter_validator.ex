defmodule FerricStore.SDK.Native.EventFilterValidator do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.SDK.Native.EventIdentifier

  @max_event_filters RequestLimits.max_batch_items()
  @deadline_check_interval 256

  @spec validate(term(), DeadlineBudget.t()) :: :ok | {:error, term()}
  def validate(events, budget) when is_list(events), do: walk(events, 0, 0, budget)
  def validate(_events, _budget), do: {:error, {:invalid_event_list, :expected_list}}

  defp walk([], _index, _until_check, budget), do: DeadlineBudget.ensure_active(budget)

  defp walk([_event | _events], @max_event_filters, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      {:error,
       {:event_list_too_large, %{items: @max_event_filters + 1, limit: @max_event_filters}}}
    end
  end

  defp walk(events, index, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      walk(events, index, @deadline_check_interval, budget)
    end
  end

  defp walk([event | events], index, until_check, budget) do
    case EventIdentifier.normalize(event) do
      {:ok, _normalized} -> walk(events, index + 1, until_check - 1, budget)
      {:error, reason} -> invalid(event, index, reason)
    end
  end

  defp walk(_improper_tail, _index, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: {:error, {:invalid_event_list, :improper_list}}
  end

  defp invalid(event, index, reason),
    do: {:error, {:invalid_event_filter, %{index: index, reason: reason, value: event}}}
end
