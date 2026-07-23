defmodule FerricStore.Flow.QueryCollectionCommands do
  @moduledoc false

  alias FerricStore.Flow.{
    ArgumentValidator,
    CommandRuntime,
    QueryBuilder,
    QueryRequest,
    QueryResult,
    Response
  }

  alias FerricStore.Result

  def execute(client, operation, opts, builder) do
    CommandRuntime.with_options(operation, opts, fn opts, context ->
      with {:ok, query, params} <- builder.(opts),
           {:ok, payload} <- QueryRequest.payload(query, params),
           {:ok, %QueryResult{records: records}} <-
             QueryRequest.execute_context(client, payload, context, :result) do
        Response.decode_list(records, opts, context, operation)
      else
        {:error, reason} -> Result.error(reason)
        other -> Result.error({:invalid_flow_query_collection_result, other})
      end
    end)
  end

  def typed(client, operation, type, opts, builder) do
    case ArgumentValidator.validate(operation, :type, type) do
      :ok -> execute(client, operation, put_option(opts, :type, type), builder)
      {:error, reason} -> Result.error(reason)
    end
  end

  def lineage(client, operation, kind, id, opts) do
    case ArgumentValidator.validate(operation, :id, id) do
      :ok -> execute(client, operation, opts, &QueryBuilder.lineage(kind, id, &1))
      {:error, reason} -> Result.error(reason)
    end
  end

  defp put_option(opts, key, value) when is_list(opts), do: Keyword.put(opts, key, value)
  defp put_option(opts, _key, _value), do: opts
end
