defmodule FerricStore.Flow.QueryResponse.IndexContract do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.{
    IndexFieldContract,
    IndexLifecycleContract,
    IndexStatisticsContract
  }

  alias FerricStore.Flow.QueryResponse.Validation, as: V

  @identifier ~r/\A[A-Za-z0-9_.:-]+\z/

  def validate(status, expected_id \\ nil) do
    identities = Enum.map(status.indexes, &{&1.id, &1.version})

    cond do
      identities != Enum.sort(Enum.uniq(identities)) ->
        fail(status, :identity_order)

      expected_id != nil and
          (identities == [] or Enum.any?(status.indexes, &(&1.id != expected_id))) ->
        fail(status, :filtered_identity)

      true ->
        case validate_indexes(status) do
          :ok -> IndexStatisticsContract.validate_service(status)
          {:error, _reason} = error -> error
        end
    end
  end

  defp validate_indexes(status) do
    Enum.reduce_while(status.indexes, :ok, fn index, :ok ->
      case validate_index(index, status) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_index(index, status) do
    with true <- valid_identifier?(index.id),
         true <- Enum.all?(index.workloads, &valid_identifier?/1),
         :ok <- IndexFieldContract.validate(index),
         true <- index.count_prefixes != [] == not is_nil(index.format.counter),
         :ok <- IndexLifecycleContract.validate(index),
         :ok <- IndexStatisticsContract.validate(index.statistics, status) do
      :ok
    else
      false -> fail(status, :index_contract)
      {:error, _reason} = error -> error
    end
  end

  defp valid_identifier?(value),
    do: is_binary(value) and byte_size(value) in 1..64 and Regex.match?(@identifier, value)

  defp fail(status, reason), do: V.invalid({:index_contract, reason}, status.raw)
end
