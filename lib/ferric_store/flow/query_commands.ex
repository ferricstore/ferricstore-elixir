defmodule FerricStore.Flow.QueryCommands do
  @moduledoc false

  alias FerricStore.Flow.{
    ArgumentValidator,
    CommandRuntime,
    HistoryResponse,
    Payload,
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
    do: request(client, :list, :flow_list, opts, &Payload.list_payload/1, {:list, :list})

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
    do: request(client, :search, :flow_search, opts, &Payload.search_payload/1, {:list, :search})

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
