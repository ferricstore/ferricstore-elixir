defmodule FerricStore.Protocol.CompactValueDecoder do
  @moduledoc false

  alias FerricStore.Protocol.{CompactBinaryCollections, DecodeBudget, ValueCodec}
  alias FerricStore.RequestLimits

  @max_collection_items RequestLimits.max_batch_items()
  @flow_record_fields ~w(
    id type state version priority partition_key payload_ref result_ref error_ref
    payload result error created_at_ms updated_at_ms next_run_at_ms lease_deadline_ms
    lease_owner lease_token fencing_token attempts history_max_events history_hot_max_events
    child_groups parent_flow_id parent_partition_key root_flow_id correlation_id
    terminal_retention_until_ms ttl_ms retention_ttl_ms run_state value_refs values
    payload_omitted payload_size result_omitted result_size error_omitted error_size
    max_attempts attributes
  )
  @flow_record_field_names @flow_record_fields
                           |> Enum.with_index(1)
                           |> Map.new(fn {name, id} -> {id, name} end)

  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(payload) do
    case decode_budgeted(payload, DecodeBudget.new()) do
      {:ok, value, <<>>, _budget} -> {:ok, value}
      {:ok, _value, _rest, _budget} -> {:error, :trailing_compact_response_bytes}
      {:error, _reason} = error -> error
    end
  end

  @spec take_flow_record(binary()) :: {:ok, map(), binary()} | {:error, term()}
  def take_flow_record(bytes), do: without_budget(take_flow_record(bytes, DecodeBudget.new()))

  @doc false
  def take_flow_record(<<0x84, count::32, rest::binary>>, budget)
      when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_flow_record_entries(count, rest, %{}, budget)
    end
  end

  def take_flow_record(<<0x84, _count::32, _rest::binary>>, _budget),
    do: {:error, :collection_too_large}

  def take_flow_record(_bytes, _budget), do: {:error, :invalid_compact_flow_record}

  @spec take_flow_record_list(binary()) :: {:ok, [map()], binary()} | {:error, term()}
  def take_flow_record_list(bytes),
    do: without_budget(take_flow_record_list(bytes, DecodeBudget.new()))

  @doc false
  def take_flow_record_list(<<0x85, count::32, rest::binary>>, budget)
      when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(budget, count) do
      take_records(count, rest, [], budget)
    end
  end

  def take_flow_record_list(<<0x85, _count::32, _rest::binary>>, _budget),
    do: {:error, :collection_too_large}

  def take_flow_record_list(_bytes, _budget),
    do: {:error, :invalid_compact_flow_record_list}

  defdelegate take_binary_list(bytes), to: CompactBinaryCollections
  defdelegate take_binary_list(bytes, budget), to: CompactBinaryCollections
  defdelegate take_binary_map(bytes), to: CompactBinaryCollections
  defdelegate take_binary_map(bytes, budget), to: CompactBinaryCollections
  defdelegate read_binary(bytes), to: CompactBinaryCollections
  defdelegate read_optional_binary(bytes), to: CompactBinaryCollections

  defp decode_budgeted(<<0x84, _rest::binary>> = payload, budget),
    do: take_flow_record(payload, budget)

  defp decode_budgeted(<<0x85, _rest::binary>> = payload, budget),
    do: take_flow_record_list(payload, budget)

  defp decode_budgeted(<<0x86, _rest::binary>> = payload, budget),
    do: CompactBinaryCollections.take_binary_list_list(payload, budget)

  defp decode_budgeted(<<0x87, _rest::binary>> = payload, budget),
    do: CompactBinaryCollections.take_binary_map_list(payload, budget)

  defp decode_budgeted(<<0x88, _rest::binary>> = payload, budget),
    do: CompactBinaryCollections.take_integer_list(payload, budget)

  defp decode_budgeted(_payload, _budget), do: {:error, :invalid_compact_response}

  defp take_flow_record_entries(0, rest, record, budget),
    do: {:ok, record, rest, budget}

  defp take_flow_record_entries(count, bytes, record, budget) when count > 0 do
    with {:ok, key, rest} <- take_flow_record_key(bytes),
         false <- Map.has_key?(record, key),
         {:ok, value, rest, budget} <- ValueCodec.decode_with_budget(rest, budget) do
      take_flow_record_entries(count - 1, rest, Map.put(record, key, value), budget)
    else
      true -> {:error, {:duplicate_compact_flow_field, %{bytes: duplicate_key_bytes(bytes)}}}
      {:error, _reason} = error -> error
    end
  end

  defp take_flow_record_entries(_count, _bytes, _record, _budget),
    do: {:error, :invalid_compact_flow_record_entries}

  defp take_records(0, rest, acc, budget),
    do: {:ok, Enum.reverse(acc), rest, budget}

  defp take_records(count, bytes, acc, budget) when count > 0 do
    with {:ok, record, rest, budget} <- take_flow_record(bytes, budget) do
      take_records(count - 1, rest, [record | acc], budget)
    end
  end

  defp take_records(_count, _bytes, _acc, _budget),
    do: {:error, :invalid_compact_collection}

  defp take_flow_record_key(<<0, rest::binary>>), do: read_binary(rest)

  defp take_flow_record_key(<<field_id, rest::binary>>) do
    case Map.fetch(@flow_record_field_names, field_id) do
      {:ok, name} -> {:ok, name, rest}
      :error -> {:error, {:unknown_compact_flow_field, field_id}}
    end
  end

  defp take_flow_record_key(_bytes), do: {:error, :invalid_compact_flow_field}

  defp without_budget({:ok, value, rest, _budget}), do: {:ok, value, rest}
  defp without_budget({:error, _reason} = error), do: error

  defp duplicate_key_bytes(<<0, size::32, _rest::binary>>), do: size
  defp duplicate_key_bytes(<<_field_id, _rest::binary>>), do: 0
  defp duplicate_key_bytes(_bytes), do: 0
end
