defmodule FerricStore.Flow.BatchCommands do
  @moduledoc false

  alias FerricStore.Flow.{BatchRuntime, CommandRuntime, Payload, Response}
  alias FerricStore.{Protocol, RequestContext, Result}

  def create_many(_client, [], opts), do: CommandRuntime.empty_batch(:create_many, opts)

  def create_many(client, items, opts) do
    request(
      client,
      :create_many,
      :flow_create_many,
      items,
      opts,
      &Payload.create_many_with_count/3,
      &Protocol.compact_flow_create_many_iodata_payload/2
    )
  end

  def complete_many(client, jobs, opts \\ [])

  def complete_many(_client, [], opts), do: CommandRuntime.empty_batch(:complete_many, opts)

  def complete_many(client, jobs, opts) do
    request(
      client,
      :complete_many,
      :flow_complete_many,
      jobs,
      opts,
      &Payload.complete_many_with_count/3,
      &Protocol.compact_flow_complete_many_iodata_payload/2
    )
  end

  defp request(client, operation, opcode, items, opts, payload_builder, encoder) do
    CommandRuntime.with_options(operation, opts, fn opts, context ->
      case payload_builder.(items, opts, RequestContext.budget(context)) do
        {:error, _reason} = error ->
          Result.unwrap(error)

        {:ok, payload, item_count} ->
          result =
            BatchRuntime.request(
              client,
              Protocol.opcode(opcode),
              payload,
              opts,
              encoder,
              item_count,
              context
            )

          Response.decode_record_list_or_response(result, opts, context)
      end
    end)
  end
end
