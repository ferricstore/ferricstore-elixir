defmodule FerricStore.Flow.QueryResponse.Indexes do
  @moduledoc false

  alias FerricStore.Flow.QueryIndexStatus
  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Types

  @contract "ferric.flow.query.indexes/v1"

  def decode(value) when is_map(value) do
    with {:ok, @contract} <- V.contract(value, "contract_version", @contract),
         {:ok, observed} <- V.non_negative(value, "observed_at_ms"),
         {:ok, max_age} <- V.non_negative(value, "statistics_max_age_ms"),
         {:ok, registry} <- registry(Types.get(value, "registry")),
         {:ok, services} <- V.required_map(value, "services"),
         {:ok, indexes} <- entries(Types.get(value, "indexes")) do
      {:ok,
       %QueryIndexStatus{
         contract_version: @contract,
         observed_at_ms: observed,
         statistics_max_age_ms: max_age,
         registry: registry,
         services: services,
         indexes: indexes,
         raw: value
       }}
    end
  end

  def decode(value), do: V.invalid(:indexes, value)

  defp registry(value) when is_map(value) do
    with {:ok, epoch} <- V.unsigned(value, "epoch"),
         {:ok, catalog_version} <- V.positive_unsigned(value, "catalog_version"),
         do: {:ok, %{epoch: epoch, catalog_version: catalog_version}}
  end

  defp registry(value), do: V.invalid(:registry, value)

  defp entries(entries) when is_list(entries) and length(entries) <= 32 do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case entry(entry) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, V.invalid({:index, index}, reason)}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp entries(value), do: V.invalid(:indexes, value)

  defp entry(value) when is_map(value) do
    with {:ok, id} <- V.required_binary(value, "id"),
         {:ok, version} <- V.positive_unsigned(value, "version"),
         {:ok, build_id} <- V.required_binary(value, "build_id"),
         {:ok, state} <- V.required_binary(value, "state"),
         {:ok, queryable} <- V.required_boolean(value, "queryable") do
      {:ok,
       %{
         id: id,
         version: version,
         build_id: build_id,
         state: state,
         queryable: queryable,
         raw: value
       }}
    end
  end

  defp entry(value), do: V.invalid(:index, value)
end
