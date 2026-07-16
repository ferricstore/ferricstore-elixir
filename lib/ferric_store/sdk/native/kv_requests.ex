defmodule FerricStore.SDK.Native.KVRequests do
  @moduledoc false

  alias FerricStore.Protocol
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.{RequestContext, RequestLimits, RouteKey}

  alias FerricStore.SDK.Native.{
    CoordinatorCall,
    KVBatchPreparer,
    KVBatchRequests
  }

  @default_timeout 5_000
  @max_batch_items RequestLimits.max_batch_items()

  @spec request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          RequestContext.t()
        ) ::
          {:ok, term()} | {:error, term()}
  def request_by_key(client, opcode, key, payload, %RequestContext{} = context) do
    payload = Protocol.payload_or_empty(payload)

    with :ok <- RequestContext.ensure_active(context),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, ^key} <- RouteKey.validate(key),
         {:ok, _options, item_count} <-
           RequestLimits.prepare(
             opcode,
             payload,
             RequestContext.options(context),
             RequestContext.budget(context)
           ),
         :ok <- RequestContext.ensure_active(context) do
      submit_by_key(client, opcode, key, payload, context, item_count)
    end
  end

  @spec request_by_key_with_count(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          non_neg_integer(),
          RequestContext.t()
        ) :: {:ok, term()} | {:error, term()}
  def request_by_key_with_count(
        client,
        opcode,
        key,
        payload,
        item_count,
        %RequestContext{} = context
      )
      when is_integer(item_count) and item_count >= 0 do
    with :ok <- RequestContext.ensure_active(context),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, ^key} <- RouteKey.validate(key),
         :ok <- RequestLimits.admit(item_count, @max_batch_items) do
      submit_by_key(client, opcode, key, Protocol.payload_or_empty(payload), context, item_count)
    end
  end

  @spec request_items(
          term(),
          KVBatchPreparer.operation(),
          non_neg_integer() | atom() | binary(),
          list() | map(),
          non_neg_integer(),
          RequestContext.t()
        ) :: {:ok, [map()], non_neg_integer()} | {:error, term()}
  def request_items(
        client,
        operation,
        opcode,
        items,
        item_count,
        %RequestContext{} = context
      )
      when operation in [:del, :mget, :mset] and
             (is_list(items) or (operation == :mset and is_map(items))) and
             is_integer(item_count) and item_count >= 0 do
    with :ok <- RequestContext.ensure_active(context),
         :ok <- RequestLimits.admit(item_count, @max_batch_items),
         {:ok, opcode} <- Opcodes.fetch(opcode) do
      context = RequestContext.with_batch_item_count(context, item_count)
      KVBatchRequests.dispatch(client, operation, opcode, items, item_count, context)
    end
  end

  defp submit_by_key(client, opcode, key, payload, context, item_count) do
    context = RequestContext.with_batch_item_count(context, item_count)

    with :ok <- RequestContext.ensure_active(context) do
      CoordinatorCall.submit(
        client,
        {:command, opcode, key, payload, context},
        call_timeout(context)
      )
    end
  end

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
