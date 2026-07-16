defmodule FerricStore.NativeRequestRuntime do
  @moduledoc false

  alias FerricStore.{FlowRouting, RequestContext}
  alias FerricStore.SDK.Native.PreparedRequests

  @consumed_options [:key, :route_key]

  @spec request(pid(), term(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def request(client, opcode, payload, opts) do
    with {:ok, opcode, context} <-
           PreparedRequests.prepare_native(opcode, opts, @consumed_options),
         {:ok, route} <- route(opcode, payload, opts, context) do
      submit(client, opcode, payload, context, route)
    end
  end

  @spec async_request(pid(), term(), term(), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def async_request(client, opcode, payload, opts) do
    with {:ok, opcode, context} <-
           PreparedRequests.prepare_native(opcode, opts, @consumed_options),
         {:ok, route} <- route(opcode, payload, opts, context) do
      {:ok, submit_async(client, opcode, payload, context, route)}
    end
  end

  defp route(opcode, payload, opts, context) do
    case FlowRouting.resolve_payload(opcode, payload, opts, RequestContext.budget(context)) do
      {:ok, key} -> {:ok, {:routed, key}}
      :none -> {:ok, :control}
      {:error, reason} -> {:error, reason}
    end
  end

  defp submit(client, opcode, payload, context, {:routed, key}),
    do: PreparedRequests.request_by_key(client, opcode, key, payload, context)

  defp submit(client, opcode, payload, context, :control),
    do: PreparedRequests.request(client, opcode, payload, context)

  defp submit_async(client, opcode, payload, context, {:routed, key}),
    do: PreparedRequests.async_request_by_key(client, opcode, key, payload, context)

  defp submit_async(client, opcode, payload, context, :control),
    do: PreparedRequests.async_request(client, opcode, payload, context)
end
