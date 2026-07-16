defmodule FerricStore.Protocol.CompactClaimDecoder do
  @moduledoc false

  alias FerricStore.Protocol.{CompactClaimTailDecoder, CompactValueDecoder, DecodeBudget}

  @max_collection_items FerricStore.RequestLimits.max_batch_items()
  @supported_modes [:state_attrs, :state, :attrs, :base]

  @type mode :: :state_attrs | :state | :attrs | :base

  @spec decode(binary()) :: {:ok, [list()]} | {:error, term()}
  def decode(<<count::32, rest::binary>>) when count <= @max_collection_items do
    with {:ok, budget} <- DecodeBudget.consume(DecodeBudget.new(), count) do
      try_modes(@supported_modes, count, rest, budget)
    end
  end

  def decode(<<_count::32, _rest::binary>>), do: {:error, :collection_too_large}
  def decode(_payload), do: {:error, :invalid_compact_claim_jobs}

  @spec decode(binary(), mode()) :: {:ok, [list()]} | {:error, term()}
  def decode(<<count::32, rest::binary>>, mode) when mode in @supported_modes do
    with true <- count <= @max_collection_items || {:error, :collection_too_large},
         {:ok, budget} <- DecodeBudget.consume(DecodeBudget.new(), count),
         {:ok, items, _budget} <- decode_items(count, rest, [], mode, budget) do
      {:ok, items}
    end
  end

  def decode(_payload, _mode), do: {:error, :invalid_compact_claim_jobs}

  @spec take_item(binary(), mode()) :: {:ok, list(), binary()} | {:error, term()}
  def take_item(bytes, mode), do: without_budget(take_item(bytes, mode, DecodeBudget.new()))

  @doc false
  def take_item(bytes, mode, budget) when mode in @supported_modes do
    with {:ok, id, rest} <- CompactValueDecoder.read_binary(bytes),
         {:ok, partition_key, rest} <- CompactValueDecoder.read_optional_binary(rest),
         {:ok, lease_token, <<fencing_token::signed-64, rest::binary>>} <-
           CompactValueDecoder.read_binary(rest),
         {:ok, row, rest, budget} <-
           CompactClaimTailDecoder.take(
             mode,
             [id, partition_key, lease_token, fencing_token],
             rest,
             budget
           ) do
      {:ok, row, rest, budget}
    else
      {:error, :collection_too_large} = error -> error
      _invalid -> {:error, :invalid_compact_claim_job}
    end
  end

  defp decode_items(0, <<>>, acc, _mode, budget), do: {:ok, Enum.reverse(acc), budget}

  defp decode_items(0, _rest, _acc, _mode, _budget),
    do: {:error, :trailing_compact_claim_job_bytes}

  defp decode_items(count, bytes, acc, mode, budget) do
    with {:ok, row, rest, budget} <- take_item(bytes, mode, budget) do
      decode_items(count - 1, rest, [row | acc], mode, budget)
    end
  end

  defp try_modes(modes, count, bytes, budget) do
    Enum.reduce_while(modes, {:error, :invalid_compact_claim_jobs}, fn mode, _acc ->
      case decode_items(count, bytes, [], mode, budget) do
        {:ok, items, _budget} -> {:halt, {:ok, items}}
        {:error, _reason} = error -> {:cont, error}
      end
    end)
  end

  defp without_budget({:ok, row, rest, _budget}), do: {:ok, row, rest}
  defp without_budget({:error, _reason} = error), do: error
end
