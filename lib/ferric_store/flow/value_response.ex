defmodule FerricStore.Flow.ValueResponse do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, Result}

  @deadline_check_interval 256

  @spec decode(term(), list(), module()) :: term()
  def decode({:error, _reason} = error, _refs, _codec), do: error

  def decode(values, refs, codec) when is_list(values) and is_list(refs) do
    safely_decode(codec, fn ->
      case decode_pairs(refs, values, codec, []) do
        {:ok, decoded} -> decoded
        {:error, reason} -> invalid_response(reason)
      end
    end)
  end

  def decode(_other, _refs, _codec), do: invalid_response(:expected_list)

  @spec decode_raw(term(), list(), DeadlineBudget.t()) :: term()
  def decode_raw({:error, _reason} = error, _refs, _budget), do: error

  def decode_raw(values, refs, %DeadlineBudget{} = budget)
      when is_list(values) and is_list(refs) do
    case decode_raw_pairs(refs, values, [], 0, budget) do
      {:ok, decoded} -> decoded
      {:error, :timeout} -> Result.error(:timeout)
      {:error, reason} -> invalid_response(reason)
    end
  end

  def decode_raw(_other, _refs, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: invalid_response(:expected_list)
  end

  defp decode_pairs([], [], _codec, decoded), do: {:ok, Enum.reverse(decoded)}

  defp decode_pairs([_ref | _refs], [], _codec, _decoded),
    do: {:error, :unexpected_cardinality}

  defp decode_pairs([], [_value | _values], _codec, _decoded),
    do: {:error, :unexpected_cardinality}

  defp decode_pairs([_ref | refs], [nil | values], codec, decoded),
    do: decode_pairs(refs, values, codec, [nil | decoded])

  defp decode_pairs([_ref | refs], [value | values], codec, decoded) when is_binary(value),
    do: decode_pairs(refs, values, codec, [codec.decode(value) | decoded])

  defp decode_pairs([_ref | _refs], [_value | _values], _codec, _decoded),
    do: {:error, :expected_binary_or_nil}

  defp decode_pairs(_improper_refs, _improper_values, _codec, _decoded),
    do: {:error, :expected_list}

  defp decode_raw_pairs(refs, values, decoded, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      decode_raw_pairs(refs, values, decoded, @deadline_check_interval, budget)
    end
  end

  defp decode_raw_pairs([], [], decoded, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         decoded = Enum.reverse(decoded),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, decoded}
  end

  defp decode_raw_pairs([_ref | _refs], [], _decoded, _until_check, budget),
    do: active_error(budget, :unexpected_cardinality)

  defp decode_raw_pairs([], [_value | _values], _decoded, _until_check, budget),
    do: active_error(budget, :unexpected_cardinality)

  defp decode_raw_pairs([_ref | refs], [nil | values], decoded, until_check, budget),
    do: decode_raw_pairs(refs, values, [nil | decoded], until_check - 1, budget)

  defp decode_raw_pairs([_ref | refs], [value | values], decoded, until_check, budget)
       when is_binary(value),
       do: decode_raw_pairs(refs, values, [value | decoded], until_check - 1, budget)

  defp decode_raw_pairs([_ref | _refs], [_value | _values], _decoded, _until_check, budget),
    do: active_error(budget, :expected_binary_or_nil)

  defp decode_raw_pairs(_improper_refs, _improper_values, _decoded, _until_check, budget),
    do: active_error(budget, :expected_list)

  defp active_error(budget, reason) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:error, reason}
  end

  defp safely_decode(codec, decoder) do
    decoder.()
  rescue
    _error -> Result.error({:flow_codec_decode_failed, codec})
  catch
    _kind, _reason -> Result.error({:flow_codec_decode_failed, codec})
  end

  defp invalid_response(reason),
    do: Result.error({:invalid_flow_response, %{operation: :value_mget, reason: reason}})
end
