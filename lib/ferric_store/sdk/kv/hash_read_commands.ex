defmodule FerricStore.SDK.KV.HashReadCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{Input, Response}
  alias FerricStore.SDK.Native.KVRequests

  def hget(client, key, field, opts) do
    with {:ok, field} <- Input.binary(field, :hget, :field) do
      client
      |> KVRequests.request_by_key(Opcodes.hget(), key, %{"key" => key, "field" => field}, opts)
      |> Response.binary_or_nil(:hget)
    end
  end

  def hgetall(client, key, opts) do
    client
    |> KVRequests.request_by_key(Opcodes.hgetall(), key, %{"key" => key}, opts)
    |> Response.map(:hgetall, RequestContext.budget(opts))
  end
end
