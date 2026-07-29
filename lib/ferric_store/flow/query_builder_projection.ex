defmodule FerricStore.Flow.QueryBuilderProjection do
  @moduledoc false

  alias FerricStore.Flow.QueryProjection

  @spec append(binary(), keyword()) ::
          {:ok, binary()} | {:error, {:invalid_flow_query_projection, atom()}}
  def append(query, opts) do
    case Keyword.fetch(opts, :fields) do
      :error -> {:ok, query <> " RETURN RECORDS"}
      {:ok, fields} -> QueryProjection.project(query, :records, fields)
    end
  end
end
