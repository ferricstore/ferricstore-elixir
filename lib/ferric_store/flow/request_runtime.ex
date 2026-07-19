defmodule FerricStore.Flow.RequestRuntime do
  @moduledoc false

  alias FerricStore.{FlowRouting, RequestContext, Result}
  alias FerricStore.SDK.Native.PreparedRequests

  @spec request(pid(), non_neg_integer(), term(), keyword(), RequestContext.t()) :: term()
  def request(client, opcode, payload, opts, %RequestContext{} = context) do
    client
    |> request_result(opcode, payload, opts, context)
    |> Result.unwrap()
  end

  @spec request_result(pid(), non_neg_integer(), term(), keyword(), RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def request_result(client, opcode, payload, opts, %RequestContext{} = context) do
    case FlowRouting.resolve_payload(opcode, payload, opts, RequestContext.budget(context)) do
      {:ok, key} -> PreparedRequests.request_by_key(client, opcode, key, payload, context)
      :none -> PreparedRequests.request(client, opcode, payload, context)
      {:error, reason} -> {:error, reason}
    end
  end
end
