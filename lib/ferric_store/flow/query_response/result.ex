defmodule FerricStore.Flow.QueryResponse.Result do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Flow.QueryResult
  alias FerricStore.Types

  @contract "ferric.flow.query.result/v1"

  @spec decode(term()) :: {:ok, QueryResult.t()} | {:error, term()}
  def decode(value) when is_map(value) do
    with {:ok, @contract} <- V.contract(value, "version", @contract),
         {:ok, quality} <- V.quality(Types.get(value, "quality")),
         {:ok, usage} <- V.usage(Types.get(value, "usage")) do
      shape(value, quality, usage)
    end
  end

  def decode(value), do: V.invalid(:result, value)

  defp shape(value, quality, usage) do
    case {V.has_key?(value, "records"), V.has_key?(value, "result")} do
      {true, false} -> records(value, quality, usage)
      {false, true} -> count(value, quality, usage)
      _invalid -> V.invalid(:result_shape, value)
    end
  end

  defp records(value, quality, usage) do
    records = Types.get(value, "records")

    with true <- (is_list(records) and length(records) <= 100) || {:error, :invalid_records},
         true <- Enum.all?(records, &is_map/1) || {:error, :invalid_records},
         {:ok, page} <- V.page(Types.get(value, "page")),
         :ok <- V.equal_count(usage.result_records, length(records), :records) do
      {:ok,
       %QueryResult{
         version: @contract,
         records: records,
         page: page,
         count: nil,
         quality: quality,
         usage: usage,
         raw: value
       }}
    else
      {:error, reason} -> V.invalid(:records, reason)
    end
  end

  defp count(value, quality, usage) do
    with :ok <- V.reject_key(value, "page"),
         {:ok, result} <- V.required_map(value, "result"),
         "count" <- Types.get(result, "kind") || {:error, :invalid_count_kind},
         {:ok, count} <- V.non_negative(result, "value"),
         :ok <- V.equal_count(usage.result_records, 1, :count) do
      {:ok,
       %QueryResult{
         version: @contract,
         records: nil,
         page: nil,
         count: count,
         quality: quality,
         usage: usage,
         raw: value
       }}
    else
      {:error, reason} -> V.invalid(:count, reason)
      other -> V.invalid(:count, other)
    end
  end
end
