defmodule FerricStore.Protocol.CompactClaimTailDecoder do
  @moduledoc false

  alias FerricStore.Protocol.{CompactValueDecoder, ValueCodec}

  def take(:base, row, rest, budget), do: {:ok, row, rest, budget}

  def take(:attrs, row, rest, budget) do
    case ValueCodec.decode_with_budget(rest, budget) do
      {:ok, attrs, rest, budget} when is_map(attrs) -> {:ok, row ++ [attrs], rest, budget}
      {:error, :collection_too_large} = error -> error
      _other -> {:error, :invalid_compact_claim_job_attrs}
    end
  end

  def take(:state, row, rest, budget) do
    with {:ok, run_state, rest} <- CompactValueDecoder.read_optional_binary(rest) do
      {:ok, row ++ [run_state], rest, budget}
    end
  end

  def take(:state_attrs, row, rest, budget) do
    with {:ok, run_state, rest} <- CompactValueDecoder.read_optional_binary(rest),
         {:ok, attrs, rest, budget} when is_map(attrs) <-
           ValueCodec.decode_with_budget(rest, budget) do
      {:ok, row ++ [run_state, attrs], rest, budget}
    else
      {:error, :collection_too_large} = error -> error
      _invalid -> {:error, :invalid_compact_claim_job_state_attrs}
    end
  end
end
