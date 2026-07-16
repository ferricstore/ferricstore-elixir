defmodule FerricStore.Flow.BatchRuntime do
  @moduledoc false

  alias FerricStore.Flow.RequestRuntime
  alias FerricStore.FlowRouting
  alias FerricStore.Protocol
  alias FerricStore.RequestContext
  alias FerricStore.Result
  alias FerricStore.SDK.Native.PreparedRequests

  @spec request(
          pid(),
          non_neg_integer(),
          map(),
          keyword(),
          (map(), non_neg_integer() -> {:ok, iodata()} | :error),
          non_neg_integer(),
          RequestContext.t()
        ) :: term()
  def request(client, opcode, payload, opts, compact_fun, item_count, context) do
    if Keyword.get(opts, :timeout) == :infinity do
      request_infinite(client, opcode, payload, opts, compact_fun, item_count, context)
    else
      RequestRuntime.request(client, opcode, payload, opts, context)
    end
  end

  defp request_infinite(client, opcode, payload, opts, compact_fun, item_count, context) do
    case FlowRouting.resolve_payload(opcode, payload, opts, RequestContext.budget(context)) do
      :none -> compact_request(client, opcode, payload, compact_fun, item_count, context)
      {:ok, _key} -> RequestRuntime.request(client, opcode, payload, opts, context)
      {:error, reason} -> Result.error(reason)
    end
  end

  defp compact_request(client, opcode, payload, compact_fun, item_count, context) do
    case compact_fun.(payload, item_count) do
      {:ok, compact_payload} ->
        client
        |> PreparedRequests.request_trusted_batch(
          opcode,
          Protocol.custom_payload(compact_payload),
          item_count,
          context
        )
        |> Result.unwrap()

      :error ->
        RequestRuntime.request(client, opcode, payload, [], context)
    end
  end
end
