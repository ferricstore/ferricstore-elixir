defmodule FerricStore.SDK.KV.CollectionCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.RouteKey
  alias FerricStore.SDK.KV.{Input, MultiKeyCommands, Response}
  alias FerricStore.SDK.Native.KVRequests
  @type client :: pid()

  @spec del(client(), binary() | [binary()], RequestContext.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def del(client, key_or_keys, opts), do: MultiKeyCommands.del(client, key_or_keys, opts)

  @spec mget(client(), [binary()], RequestContext.t()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def mget(client, keys, opts), do: MultiKeyCommands.mget(client, keys, opts)

  @spec mset(client(), %{binary() => binary()} | [{binary(), binary()}], RequestContext.t()) ::
          {:ok, :ok} | {:error, term()}
  def mset(client, pairs, opts), do: MultiKeyCommands.mset(client, pairs, opts)

  @spec hset(client(), binary(), %{binary() => binary()}, RequestContext.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def hset(client, key, fields, opts) do
    with {:ok, ^key} <- RouteKey.validate(key),
         {:ok, fields, item_count} <- Input.hash_fields(fields, RequestContext.budget(opts)) do
      client
      |> KVRequests.request_by_key_with_count(
        Opcodes.hset(),
        key,
        %{"key" => key, "fields" => fields},
        item_count,
        opts
      )
      |> Response.bounded_count(:hset, item_count)
    end
  end

  @spec hmget(client(), binary(), [binary()], RequestContext.t()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def hmget(client, key, fields, opts) do
    with {:ok, ^key} <- RouteKey.validate(key),
         {:ok, fields, item_count} <-
           Input.binary_list(fields, :hmget, :fields, RequestContext.budget(opts)) do
      client
      |> KVRequests.request_by_key_with_count(
        Opcodes.hmget(),
        key,
        %{"key" => key, "fields" => fields},
        item_count,
        opts
      )
      |> Response.exact_list(:hmget, item_count, RequestContext.budget(opts))
    end
  end

  @spec lpush(client(), binary(), binary() | [binary()], RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def lpush(client, key, values, opts),
    do: push(client, :lpush, Opcodes.lpush(), key, values, opts)

  @spec rpush(client(), binary(), binary() | [binary()], RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def rpush(client, key, values, opts),
    do: push(client, :rpush, Opcodes.rpush(), key, values, opts)

  @spec sadd(client(), binary(), binary() | [binary()], RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def sadd(client, key, members, opts),
    do: set_members(client, :sadd, Opcodes.sadd(), key, members, opts)

  @spec srem(client(), binary(), binary() | [binary()], RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def srem(client, key, members, opts),
    do: set_members(client, :srem, Opcodes.srem(), key, members, opts)

  @spec zadd(client(), binary(), list(), RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def zadd(client, key, items, opts) do
    with {:ok, ^key} <- RouteKey.validate(key) do
      case Input.zadd_items(items, RequestContext.budget(opts)) do
        {:ok, 0, []} ->
          {:ok, 0}

        {:ok, item_count, normalized} ->
          client
          |> KVRequests.request_by_key_with_count(
            Opcodes.zadd(),
            key,
            %{"key" => key, "items" => normalized},
            item_count,
            opts
          )
          |> Response.bounded_count(:zadd, item_count)

        {:error, _reason} = error ->
          error
      end
    end
  end

  @spec zrem(client(), binary(), binary() | [binary()], RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def zrem(client, key, members, opts),
    do: set_members(client, :zrem, Opcodes.zrem(), key, members, opts)

  defp push(client, operation, opcode, key, values, opts) do
    with {:ok, ^key} <- RouteKey.validate(key),
         {:ok, values, item_count} <-
           Input.binary_or_list(values, operation, :values, RequestContext.budget(opts)) do
      client
      |> KVRequests.request_by_key_with_count(
        opcode,
        key,
        %{"key" => key, "values" => values},
        item_count,
        opts
      )
      |> Response.non_negative_integer(operation)
    end
  end

  defp set_members(client, operation, opcode, key, members, opts) do
    with {:ok, ^key} <- RouteKey.validate(key),
         {:ok, members, item_count} <-
           Input.binary_or_list(members, operation, :members, RequestContext.budget(opts)) do
      client
      |> KVRequests.request_by_key_with_count(
        opcode,
        key,
        %{"key" => key, "members" => members},
        item_count,
        opts
      )
      |> Response.bounded_count(operation, item_count)
    end
  end
end
