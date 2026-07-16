defmodule FerricStore.SDK.KV.CollectionReadCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{Input, Response}
  alias FerricStore.SDK.Native.KVRequests

  def lpop(client, key, count, opts), do: pop(client, :lpop, Opcodes.lpop(), key, count, opts)
  def rpop(client, key, count, opts), do: pop(client, :rpop, Opcodes.rpop(), key, count, opts)

  def lrange(client, key, start, stop, opts) do
    with {:ok, start} <- Input.integer(start, :lrange, :start),
         {:ok, stop} <- Input.integer(stop, :lrange, :stop) do
      client
      |> KVRequests.request_by_key(
        Opcodes.lrange(),
        key,
        %{"key" => key, "start" => start, "stop" => stop},
        opts
      )
      |> Response.list(:lrange, RequestContext.budget(opts))
    end
  end

  def smembers(client, key, opts) do
    client
    |> KVRequests.request_by_key(Opcodes.smembers(), key, %{"key" => key}, opts)
    |> Response.list(:smembers, RequestContext.budget(opts))
  end

  def sismember(client, key, member, opts) do
    with {:ok, member} <- Input.binary(member, :sismember, :member) do
      client
      |> KVRequests.request_by_key(
        Opcodes.sismember(),
        key,
        %{"key" => key, "member" => member},
        opts
      )
      |> Response.boolean(:sismember)
    end
  end

  defp pop(client, operation, opcode, key, count, opts) do
    with {:ok, count} <- Input.collection_count(count, operation, :count) do
      client
      |> KVRequests.request_by_key(opcode, key, %{"key" => key, "count" => count}, opts)
      |> Response.pop(operation, count, RequestContext.budget(opts))
    end
  end
end
