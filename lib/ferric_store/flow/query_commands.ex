defmodule FerricStore.Flow.QueryCommands do
  @moduledoc false
  alias FerricStore.Flow.{
    ArgumentValidator,
    CommandRuntime,
    HistoryResponse,
    Payload,
    QueryBuilder,
    QueryCollectionCommands,
    QueryRequest,
    RequestRuntime,
    Response
  }

  alias FerricStore.{Protocol, Result}

  def get(client, id, opts \\ []),
    do:
      identified_request(
        client,
        :get,
        :flow_get,
        :id,
        id,
        opts,
        &Payload.get_payload(id, &1),
        {:record, :get}
      )

  def list(client, opts \\ []),
    do: QueryCollectionCommands.execute(client, :list, opts, &QueryBuilder.list/1)

  def history(client, id, opts \\ []),
    do:
      identified_request(
        client,
        :history,
        :flow_history,
        :id,
        id,
        opts,
        &Payload.history_payload(id, &1),
        {:history, :history}
      )

  def claim_due(client, type, opts),
    do:
      identified_request(
        client,
        :claim_due,
        :flow_claim_due,
        :type,
        type,
        opts,
        &Payload.claim_due_payload(type, &1),
        :claims
      )

  def search(client, opts \\ []),
    do: QueryCollectionCommands.execute(client, :search, opts, &QueryBuilder.search/1)

  def terminals(client, type, opts \\ []),
    do: QueryCollectionCommands.typed(client, :terminals, type, opts, &QueryBuilder.terminals/1)

  def failures(client, type, opts \\ []),
    do: QueryCollectionCommands.typed(client, :failures, type, opts, &QueryBuilder.failures/1)

  def by_parent(client, id, opts \\ []),
    do: QueryCollectionCommands.lineage(client, :by_parent, :parent, id, opts)

  def by_root(client, id, opts \\ []),
    do: QueryCollectionCommands.lineage(client, :by_root, :root, id, opts)

  def by_correlation(client, id, opts \\ []),
    do: QueryCollectionCommands.lineage(client, :by_correlation, :correlation, id, opts)

  def stuck(client, type, opts \\ []),
    do: QueryCollectionCommands.typed(client, :stuck, type, opts, &QueryBuilder.stuck/1)

  def query(client, query, params \\ %{}, opts \\ []),
    do: client |> QueryRequest.query(query, params, opts) |> Result.unwrap()

  def explain(client, query, params \\ %{}, opts \\ []),
    do: client |> QueryRequest.explain(query, params, opts) |> Result.unwrap()

  def explain_analyze(client, query, params \\ %{}, opts \\ []),
    do: client |> QueryRequest.explain_analyze(query, params, opts) |> Result.unwrap()

  def query_indexes(client, index_id \\ nil, opts \\ []),
    do: client |> QueryRequest.indexes(index_id, opts) |> Result.unwrap()

  defp identified_request(client, operation, opcode, field, value, opts, payload_builder, decoder) do
    case ArgumentValidator.validate(operation, field, value) do
      :ok -> request(client, operation, opcode, opts, payload_builder, decoder)
      {:error, reason} -> Result.error(reason)
    end
  end

  defp request(client, operation, opcode, opts, payload_builder, decoder) do
    CommandRuntime.with_options(operation, opts, fn opts, context ->
      result =
        RequestRuntime.request(
          client,
          Protocol.opcode(opcode),
          payload_builder.(opts),
          opts,
          context
        )

      decode(result, decoder, opts, context)
    end)
  end

  defp decode(result, {:record, operation}, opts, context),
    do: Response.decode_record(result, opts, context, operation)

  defp decode(result, {:list, operation}, opts, context),
    do: Response.decode_list(result, opts, context, operation)

  defp decode(result, {:history, operation}, opts, context),
    do: HistoryResponse.decode(result, opts, context, operation)

  defp decode(result, :claims, opts, context), do: Response.decode_claims(result, opts, context)
end
