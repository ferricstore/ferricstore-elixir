defmodule FerricStore.Protocol.CompactPipelineItemDecoder do
  @moduledoc false

  alias FerricStore.BinaryDetacher
  alias FerricStore.Protocol.CompactValueDecoder

  def decode(<<0, 0, rest::binary>>, budget), do: {:ok, ["ok", nil], rest, budget}

  def decode(<<0, 1, size::32, value::binary-size(size), rest::binary>>, budget),
    do: {:ok, ["ok", BinaryDetacher.detach(value)], rest, budget}

  def decode(<<0, 2, rest::binary>>, budget),
    do: decode_with(rest, budget, &CompactValueDecoder.take_flow_record/2)

  def decode(<<0, 3, rest::binary>>, budget),
    do: decode_with(rest, budget, &CompactValueDecoder.take_flow_record_list/2)

  def decode(<<0, 4, rest::binary>>, _budget), do: {:claim, rest}

  def decode(<<0, 5, rest::binary>>, budget) do
    with {:ok, ref, rest} <- CompactValueDecoder.read_binary(rest),
         {:ok, partition_key, rest} <- CompactValueDecoder.read_optional_binary(rest),
         {:ok, owner_flow_id, rest} <- CompactValueDecoder.read_optional_binary(rest) do
      value = %{
        "ref" => ref,
        "partition_key" => partition_key,
        "owner_flow_id" => owner_flow_id
      }

      {:ok, ["ok", value], rest, budget}
    end
  end

  def decode(<<0, 6, rest::binary>>, budget),
    do: decode_with(rest, budget, &CompactValueDecoder.take_binary_list/2)

  def decode(<<0, 7, rest::binary>>, budget),
    do: decode_with(rest, budget, &CompactValueDecoder.take_binary_map/2)

  def decode(<<status, size::32, reason::binary-size(size), rest::binary>>, budget)
      when status in [1, 2] do
    label = if status == 1, do: "busy", else: "error"
    {:ok, [label, BinaryDetacher.detach(reason)], rest, budget}
  end

  def decode(_bytes, _budget), do: {:error, :invalid_compact_pipeline_item}

  defp decode_with(bytes, budget, decoder) do
    case decoder.(bytes, budget) do
      {:ok, value, rest, budget} -> {:ok, ["ok", value], rest, budget}
      {:error, _reason} = error -> error
    end
  end
end
