defmodule FerricStore.Flow.HistoryResponseDecoder do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, Result}
  alias FerricStore.Flow.{ResponseRecords, ResponseResultList}

  @spec decode_raw(term(), atom(), DeadlineBudget.t()) :: term()
  def decode_raw(entries, operation, %DeadlineBudget{} = budget) when is_list(entries) do
    case ResponseResultList.map(entries, budget, &entry_result/1) do
      {:ok, history} -> history
      {:error, :timeout} -> Result.error(:timeout)
      {:error, :invalid_history_entry} -> invalid(operation, :invalid_history_entry)
      {:error, :improper_list} -> invalid(operation, :expected_history_list)
    end
  end

  def decode_raw(_entries, operation, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid(operation, :expected_history_list)
  end

  @spec decode(term(), atom(), module()) :: term()
  def decode(entries, operation, codec) when is_list(entries),
    do: decode_entries(entries, operation, codec, [])

  def decode(_entries, operation, _codec),
    do: invalid(operation, :expected_history_list)

  defp decode_entries([], _operation, _codec, decoded), do: Enum.reverse(decoded)

  defp decode_entries([[event_id, record] | entries], operation, codec, decoded)
       when is_binary(event_id) and is_map(record) do
    entry = {event_id, ResponseRecords.decode_record(record, codec)}
    decode_entries(entries, operation, codec, [entry | decoded])
  end

  defp decode_entries([_entry | _entries], operation, _codec, _decoded),
    do: invalid(operation, :invalid_history_entry)

  defp decode_entries(_improper, operation, _codec, _decoded),
    do: invalid(operation, :expected_history_list)

  defp entry_result([event_id, record]) when is_binary(event_id) and is_map(record),
    do: {:ok, {event_id, record}}

  defp entry_result(_entry), do: {:error, :invalid_history_entry}

  defp invalid(operation, reason),
    do: Result.error({:invalid_flow_response, %{operation: operation, reason: reason}})
end
