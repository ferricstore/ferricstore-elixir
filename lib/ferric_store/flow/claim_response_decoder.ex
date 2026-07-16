defmodule FerricStore.Flow.ClaimResponseDecoder do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, Result}
  alias FerricStore.Flow.{ClaimNormalizer, ResponseRecords, ResponseResultList}

  @spec decode_raw(list(), DeadlineBudget.t()) :: list() | {:error, FerricStore.Error.t()}
  def decode_raw(jobs, %DeadlineBudget{} = budget) do
    case ResponseResultList.map(jobs, budget, &ClaimNormalizer.normalize/1) do
      {:ok, normalized} -> normalized
      {:error, :timeout} -> Result.error(:timeout)
      {:error, :invalid_claim} -> invalid(:invalid_claim)
      {:error, :improper_list} -> invalid(:expected_list)
    end
  end

  @spec decode(list(), module()) :: list() | {:error, FerricStore.Error.t()}
  def decode(jobs, codec) do
    case decode_items(jobs, codec, []) do
      {:ok, decoded} -> Enum.reverse(decoded)
      {:error, reason} -> invalid(reason)
    end
  end

  defp decode_items([], _codec, decoded), do: {:ok, decoded}

  defp decode_items([job | jobs], codec, decoded) do
    case ClaimNormalizer.normalize(job) do
      {:ok, record} ->
        decode_items(jobs, codec, [ResponseRecords.decode_record(record, codec) | decoded])

      {:error, :invalid_claim} ->
        {:error, :invalid_claim}
    end
  end

  defp decode_items(_improper, _codec, _decoded), do: {:error, :expected_list}

  @spec invalid(atom()) :: {:error, FerricStore.Error.t()}
  def invalid(reason),
    do: Result.error({:invalid_flow_response, %{operation: :claim_due, reason: reason}})
end
