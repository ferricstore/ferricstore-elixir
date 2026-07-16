defmodule FerricStore.SDK.Native.ClientBatchRequests do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.{RequestContext, RequestLimits}
  alias FerricStore.SDK.Native.{ClientRequestAdmission, CoordinatorCall}

  @default_timeout 5_000
  @max_batch_items RequestLimits.max_batch_items()
  @trusted_request_options [:timeout, :call_timeout, :idempotent, :lane_id, :endpoint]

  @spec request_trusted(term(), term(), term(), term(), term()) ::
          {:ok, term()} | {:error, term()}
  def request_trusted(
        client,
        opcode,
        {:custom_payload, body} = payload,
        item_count,
        opts
      )
      when (is_binary(body) or is_list(body)) and is_integer(item_count) and item_count >= 0 do
    with {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, context} <-
           ClientRequestAdmission.context(opts, @default_timeout, @trusted_request_options),
         :ok <- RequestContext.ensure_active(context),
         :ok <- RequestLimits.admit(item_count, @max_batch_items) do
      context = RequestContext.with_batch_item_count(context, item_count)

      CoordinatorCall.submit(
        client,
        {:request, opcode, payload, context},
        RequestContext.call_timeout(context, @default_timeout)
      )
    end
  end

  def request_trusted(_client, _opcode, payload, item_count, _opts),
    do: {:error, {:invalid_trusted_batch, %{payload: payload, item_count: item_count}}}

  @spec request_with_count(
          term(),
          non_neg_integer(),
          list(),
          (term() -> binary()),
          (list() -> map()),
          RequestContext.t()
        ) :: {:ok, [map()], non_neg_integer()} | {:error, term()}
  def request_with_count(
        client,
        opcode,
        items,
        key_fun,
        payload_builder,
        %RequestContext{} = context
      ) do
    with {:ok, item_count} <- ClientRequestAdmission.count_batch_items(items, context) do
      submit(client, opcode, items, item_count, key_fun, payload_builder, context)
    end
  end

  defp submit(client, opcode, items, item_count, key_fun, payload_builder, context) do
    with :ok <- RequestContext.ensure_active(context) do
      context = RequestContext.with_batch_item_count(context, item_count)

      case CoordinatorCall.submit(
             client,
             {:command_items, opcode, items, item_count, key_fun, payload_builder, context},
             RequestContext.call_timeout(context, @default_timeout)
           ) do
        {:ok, groups} -> {:ok, groups, item_count}
        {:error, _reason} = error -> error
      end
    end
  end
end
