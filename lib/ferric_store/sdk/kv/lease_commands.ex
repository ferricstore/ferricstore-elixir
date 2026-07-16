defmodule FerricStore.SDK.KV.LeaseCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.KV.{Input, Response}
  alias FerricStore.SDK.Native.KVRequests

  def lock(client, key, owner, ttl_ms, opts) do
    with {:ok, owner} <- Input.binary(owner, :lock, :owner),
         {:ok, ttl_ms} <- Input.positive_integer(ttl_ms, :lock, :ttl_ms) do
      client
      |> KVRequests.request_by_key(
        Opcodes.lock(),
        key,
        %{"key" => key, "owner" => owner, "ttl_ms" => ttl_ms},
        opts
      )
      |> Response.ok(:lock)
    end
  end

  def unlock(client, key, owner, opts) do
    with {:ok, owner} <- Input.binary(owner, :unlock, :owner) do
      client
      |> KVRequests.request_by_key(
        Opcodes.unlock(),
        key,
        %{"key" => key, "owner" => owner},
        opts
      )
      |> Response.one(:unlock)
    end
  end

  def extend(client, key, owner, ttl_ms, opts) do
    with {:ok, owner} <- Input.binary(owner, :extend, :owner),
         {:ok, ttl_ms} <- Input.positive_integer(ttl_ms, :extend, :ttl_ms) do
      client
      |> KVRequests.request_by_key(
        Opcodes.extend(),
        key,
        %{"key" => key, "owner" => owner, "ttl_ms" => ttl_ms},
        opts
      )
      |> Response.one(:extend)
    end
  end
end
