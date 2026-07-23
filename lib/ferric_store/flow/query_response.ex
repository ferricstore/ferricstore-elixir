defmodule FerricStore.Flow.QueryResponse do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.{Diagnostic, Explain, Indexes, Result}

  defdelegate result(value), to: Result, as: :decode
  defdelegate explain(value), to: Explain, as: :decode
  defdelegate indexes(value), to: Indexes, as: :decode

  @spec diagnostic(term(), term()) :: {:ok, FerricStore.Flow.QueryError.t()} | :error
  def diagnostic(reason, raw \\ nil), do: Diagnostic.from_reason(reason, raw || reason)
end
