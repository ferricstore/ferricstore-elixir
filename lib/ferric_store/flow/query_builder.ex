defmodule FerricStore.Flow.QueryBuilder do
  @moduledoc false

  alias FerricStore.Flow.{QueryBuilderCollections, QueryBuilderListSearch}

  defdelegate list(opts), to: QueryBuilderListSearch
  defdelegate search(opts), to: QueryBuilderListSearch
  defdelegate terminals(opts), to: QueryBuilderCollections
  defdelegate failures(opts), to: QueryBuilderCollections
  defdelegate lineage(kind, id, opts), to: QueryBuilderCollections
  defdelegate stuck(opts), to: QueryBuilderCollections
end
