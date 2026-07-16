defmodule FerricStore.Flow.RecordResponseDecoder do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, Result}
  alias FerricStore.Flow.{ResponseRecords, ResponseResultList}

  @spec decode_record_raw(term(), atom(), DeadlineBudget.t()) :: term()
  def decode_record_raw(value, operation, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      if is_nil(value) or is_map(value),
        do: value,
        else: invalid(operation, :expected_record_or_nil)
    end
  end

  @spec decode_record(term(), atom(), module()) :: term()
  def decode_record(value, _operation, codec) when is_map(value),
    do: ResponseRecords.decode_record(value, codec)

  def decode_record(nil, _operation, _codec), do: nil
  def decode_record(_value, operation, _codec), do: invalid(operation, :expected_record_or_nil)

  @spec decode_list_raw(term(), atom(), DeadlineBudget.t()) :: term()
  def decode_list_raw(values, operation, %DeadlineBudget{} = budget) when is_list(values) do
    case ResponseResultList.map(values, budget, &record_result/1) do
      {:ok, records} -> records
      {:error, :timeout} -> Result.error(:timeout)
      {:error, :expected_record_map} -> invalid(operation, :expected_record_map)
      {:error, :improper_list} -> invalid(operation, :expected_record_list)
    end
  end

  def decode_list_raw(_values, operation, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid(operation, :expected_record_list)
  end

  @spec decode_list(term(), atom(), module()) :: term()
  def decode_list(values, operation, codec) when is_list(values),
    do: decode_list_items(values, operation, codec, [])

  def decode_list(_values, operation, _codec), do: invalid(operation, :expected_record_list)

  defp decode_list_items([], _operation, _codec, decoded), do: Enum.reverse(decoded)

  defp decode_list_items([record | records], operation, codec, decoded) when is_map(record),
    do:
      decode_list_items(records, operation, codec, [
        ResponseRecords.decode_record(record, codec) | decoded
      ])

  defp decode_list_items([_record | _records], operation, _codec, _decoded),
    do: invalid(operation, :expected_record_map)

  defp decode_list_items(_improper, operation, _codec, _decoded),
    do: invalid(operation, :expected_record_list)

  defp record_result(record) when is_map(record), do: {:ok, record}
  defp record_result(_record), do: {:error, :expected_record_map}

  defp invalid(operation, reason),
    do: Result.error({:invalid_flow_response, %{operation: operation, reason: reason}})
end
