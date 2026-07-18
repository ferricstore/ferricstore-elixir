defmodule FerricStore.SDK.Native.ServerResponseCodecs do
  @moduledoc false

  alias FerricStore.Types

  @supported MapSet.new(~w(
    flow_claim_jobs_v1
    flow_record_list_v1
    flow_record_v1
    kv_get_v1
    kv_mget_v1
    ok_list_v1
    pipeline_v1
  ))
  @max_codecs 32
  @max_opcodes 1_024

  @spec parse(term()) :: {:ok, %{non_neg_integer() => binary()}} | {:error, term()}
  def parse(capabilities) when is_map(capabilities) do
    with response_codecs when is_map(response_codecs) <-
           Types.get(capabilities, "response_codecs"),
         table when is_map(table) <-
           Types.get(response_codecs, "compact_response_opcodes"),
         :ok <- bounded_map(table),
         {:ok, indexed, _count} <- index_table(table, %{}, 0) do
      {:ok, indexed}
    else
      nil -> {:error, :missing}
      {:error, _reason} = error -> error
      _invalid -> {:error, :expected_map}
    end
  end

  def parse(_capabilities), do: {:error, :expected_capabilities_map}

  defp bounded_map(table) when map_size(table) <= @max_codecs, do: :ok
  defp bounded_map(_table), do: {:error, :too_many_codecs}

  defp index_table(table, indexed, count) do
    Enum.reduce_while(table, {:ok, indexed, count}, fn {codec, opcodes}, {:ok, acc, total} ->
      case index_codec(codec, opcodes, acc, total) do
        {:ok, _acc, _total} = ok -> {:cont, ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp index_codec(codec, opcodes, indexed, count)
       when is_binary(codec) and is_list(opcodes) do
    cond do
      not String.valid?(codec) ->
        {:error, :invalid_codec_name}

      not MapSet.member?(@supported, codec) ->
        {:error, {:unsupported_codec, codec}}

      true ->
        index_opcodes(opcodes, codec, indexed, count)
    end
  end

  defp index_codec(codec, _opcodes, _indexed, _count) when not is_binary(codec),
    do: {:error, :invalid_codec_name}

  defp index_codec(codec, _opcodes, _indexed, _count),
    do: {:error, {:invalid_opcode_list, codec}}

  defp index_opcodes([], _codec, indexed, count), do: {:ok, indexed, count}

  defp index_opcodes([opcode | rest], codec, indexed, count)
       when is_integer(opcode) and opcode in 0..0xFFFF do
    cond do
      count >= @max_opcodes ->
        {:error, :too_many_opcodes}

      Map.has_key?(indexed, opcode) ->
        {:error, {:duplicate_opcode, opcode}}

      true ->
        index_opcodes(rest, codec, Map.put(indexed, opcode, codec), count + 1)
    end
  end

  defp index_opcodes([opcode | _rest], _codec, _indexed, _count),
    do: {:error, {:invalid_opcode, opcode}}

  defp index_opcodes(_improper, codec, _indexed, _count),
    do: {:error, {:invalid_opcode_list, codec}}
end
