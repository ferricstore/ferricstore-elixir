defmodule FerricStore.Flow.QueryResponse.Indexes do
  @moduledoc false

  alias FerricStore.Flow.{QueryIndexServices, QueryIndexStatus}

  alias FerricStore.Flow.QueryResponse.{
    IndexContract,
    IndexDefinition,
    Validation
  }

  alias FerricStore.Types

  @contract "ferric.flow.query.indexes/v1"

  def decode(value, expected_id \\ nil)

  def decode(value, expected_id) when is_map(value) do
    with {:ok, @contract} <- Validation.contract(value, "contract_version", @contract),
         {:ok, observed} <- Validation.unsigned(value, "observed_at_ms"),
         {:ok, max_age} <- Validation.unsigned(value, "statistics_max_age_ms"),
         {:ok, registry} <- registry(Types.get(value, "registry")),
         {:ok, services} <- services(Types.get(value, "services")),
         {:ok, indexes} <- entries(Types.get(value, "indexes")),
         status = %QueryIndexStatus{
           contract_version: @contract,
           observed_at_ms: observed,
           statistics_max_age_ms: max_age,
           registry: registry,
           services: services,
           indexes: indexes,
           raw: value
         },
         :ok <- IndexContract.validate(status, expected_id) do
      {:ok, status}
    end
  end

  def decode(value, _expected_id), do: Validation.invalid(:indexes, value)

  defp registry(value) when is_map(value) do
    with {:ok, epoch} <- Validation.unsigned(value, "epoch"),
         {:ok, catalog_version} <- Validation.positive_unsigned(value, "catalog_version"),
         do: {:ok, %{epoch: epoch, catalog_version: catalog_version}}
  end

  defp registry(value), do: Validation.invalid(:registry, value)

  defp services(value) when is_map(value) do
    with {:ok, registry} <- choice(value, "registry"),
         {:ok, lifecycle_worker} <- choice(value, "lifecycle_worker"),
         {:ok, statistics_store} <- choice(value, "statistics_store"),
         {:ok, statistics_worker} <- choice(value, "statistics_worker") do
      {:ok,
       %QueryIndexServices{
         registry: registry,
         lifecycle_worker: lifecycle_worker,
         statistics_store: statistics_store,
         statistics_worker: statistics_worker,
         raw: value
       }}
    end
  end

  defp services(value), do: Validation.invalid(:services, value)

  defp entries(entries) when is_list(entries) and length(entries) <= 32 do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case IndexDefinition.decode(entry) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, Validation.invalid({:index, index}, reason)}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp entries(value), do: Validation.invalid(:indexes, value)

  defp choice(value, field) do
    actual = Types.get(value, field)

    if actual in ~w(ready unavailable),
      do: {:ok, actual},
      else: Validation.invalid(:services, actual)
  end
end
