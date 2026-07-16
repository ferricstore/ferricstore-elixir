defmodule FerricStore.SDK.KV do
  @moduledoc """
  Native FerricStore key/value commands.
  """

  alias FerricStore.SDK.KV.{CollectionCommands, Runtime, ScalarCommands}

  @type client :: pid()

  @spec get(client(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, term()}
  def get(client, key, opts \\ []),
    do: Runtime.call(:get, opts, &ScalarCommands.get(client, key, &1))

  @spec set(client(), binary(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def set(client, key, value, opts \\ []),
    do: Runtime.call(:set, opts, &ScalarCommands.set(client, key, value, &1))

  @spec del(client(), binary() | [binary()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def del(client, keys, opts \\ []),
    do: Runtime.call(:del, opts, &CollectionCommands.del(client, keys, &1))

  @spec mget(client(), [binary()], keyword()) :: {:ok, [binary() | nil]} | {:error, term()}
  def mget(client, keys, opts \\ []),
    do: Runtime.call(:mget, opts, &CollectionCommands.mget(client, keys, &1))

  @spec mset(client(), %{binary() => binary()} | [{binary(), binary()}], keyword()) ::
          {:ok, :ok} | {:error, term()}
  def mset(client, pairs, opts \\ []),
    do: Runtime.call(:mset, opts, &CollectionCommands.mset(client, pairs, &1))

  @spec cas(client(), binary(), binary(), binary(), keyword()) ::
          {:ok, boolean() | nil} | {:error, term()}
  def cas(client, key, expected, value, opts \\ []),
    do: Runtime.call(:cas, opts, &ScalarCommands.cas(client, key, expected, value, &1))

  @spec lock(client(), binary(), binary(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def lock(client, key, owner, ttl_ms, opts \\ []),
    do: Runtime.call(:lock, opts, &ScalarCommands.lock(client, key, owner, ttl_ms, &1))

  @spec unlock(client(), binary(), binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def unlock(client, key, owner, opts \\ []),
    do: Runtime.call(:unlock, opts, &ScalarCommands.unlock(client, key, owner, &1))

  @spec extend(client(), binary(), binary(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def extend(client, key, owner, ttl_ms, opts \\ []),
    do: Runtime.call(:extend, opts, &ScalarCommands.extend(client, key, owner, ttl_ms, &1))

  @spec ratelimit_add(client(), binary(), pos_integer(), pos_integer(), pos_integer(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def ratelimit_add(client, key, window_ms, max, count \\ 1, opts \\ []),
    do:
      Runtime.call(
        :ratelimit_add,
        opts,
        &ScalarCommands.ratelimit_add(client, key, window_ms, max, count, &1)
      )

  @spec fetch_or_compute(client(), binary(), pos_integer(), keyword()) ::
          {:ok, [term()]} | {:error, term()}
  def fetch_or_compute(client, key, ttl_ms, opts \\ []),
    do:
      Runtime.call(
        :fetch_or_compute,
        opts,
        &ScalarCommands.fetch_or_compute(client, key, ttl_ms, &1)
      )

  @spec fetch_or_compute_result(
          client(),
          binary(),
          binary(),
          binary(),
          pos_integer(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def fetch_or_compute_result(client, key, token, value, ttl_ms, opts \\ []),
    do:
      Runtime.call(
        :fetch_or_compute_result,
        opts,
        &ScalarCommands.fetch_or_compute_result(client, key, token, value, ttl_ms, &1)
      )

  @spec fetch_or_compute_error(client(), binary(), binary(), binary(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def fetch_or_compute_error(client, key, token, message, opts \\ []),
    do:
      Runtime.call(
        :fetch_or_compute_error,
        opts,
        &ScalarCommands.fetch_or_compute_error(client, key, token, message, &1)
      )

  @spec hset(client(), binary(), %{binary() => binary()}, keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def hset(client, key, fields, opts \\ []),
    do: Runtime.call(:hset, opts, &CollectionCommands.hset(client, key, fields, &1))

  @spec hget(client(), binary(), binary(), keyword()) ::
          {:ok, binary() | nil} | {:error, term()}
  def hget(client, key, field, opts \\ []),
    do: Runtime.call(:hget, opts, &ScalarCommands.hget(client, key, field, &1))

  @spec hmget(client(), binary(), [binary()], keyword()) ::
          {:ok, [binary() | nil]} | {:error, term()}
  def hmget(client, key, fields, opts \\ []),
    do: Runtime.call(:hmget, opts, &CollectionCommands.hmget(client, key, fields, &1))

  @spec hgetall(client(), binary(), keyword()) :: {:ok, map()} | {:error, term()}
  def hgetall(client, key, opts \\ []),
    do: Runtime.call(:hgetall, opts, &ScalarCommands.hgetall(client, key, &1))

  @spec lpush(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def lpush(client, key, values, opts \\ []),
    do: Runtime.call(:lpush, opts, &CollectionCommands.lpush(client, key, values, &1))

  @spec rpush(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def rpush(client, key, values, opts \\ []),
    do: Runtime.call(:rpush, opts, &CollectionCommands.rpush(client, key, values, &1))

  @spec lpop(client(), binary(), pos_integer(), keyword()) :: {:ok, term()} | {:error, term()}
  def lpop(client, key, count \\ 1, opts \\ []),
    do: Runtime.call(:lpop, opts, &ScalarCommands.lpop(client, key, count, &1))

  @spec rpop(client(), binary(), pos_integer(), keyword()) :: {:ok, term()} | {:error, term()}
  def rpop(client, key, count \\ 1, opts \\ []),
    do: Runtime.call(:rpop, opts, &ScalarCommands.rpop(client, key, count, &1))

  @spec lrange(client(), binary(), integer(), integer(), keyword()) ::
          {:ok, [term()]} | {:error, term()}
  def lrange(client, key, start, stop, opts \\ []),
    do: Runtime.call(:lrange, opts, &ScalarCommands.lrange(client, key, start, stop, &1))

  @spec sadd(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def sadd(client, key, members, opts \\ []),
    do: Runtime.call(:sadd, opts, &CollectionCommands.sadd(client, key, members, &1))

  @spec srem(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def srem(client, key, members, opts \\ []),
    do: Runtime.call(:srem, opts, &CollectionCommands.srem(client, key, members, &1))

  @spec smembers(client(), binary(), keyword()) :: {:ok, [term()]} | {:error, term()}
  def smembers(client, key, opts \\ []),
    do: Runtime.call(:smembers, opts, &ScalarCommands.smembers(client, key, &1))

  @spec sismember(client(), binary(), binary(), keyword()) ::
          {:ok, boolean()} | {:error, term()}
  def sismember(client, key, member, opts \\ []),
    do: Runtime.call(:sismember, opts, &ScalarCommands.sismember(client, key, member, &1))

  @spec zadd(client(), binary(), list(), keyword()) :: {:ok, term()} | {:error, term()}
  def zadd(client, key, items, opts \\ []),
    do: Runtime.call(:zadd, opts, &CollectionCommands.zadd(client, key, items, &1))

  @spec zrem(client(), binary(), binary() | [binary()], keyword()) ::
          {:ok, term()} | {:error, term()}
  def zrem(client, key, members, opts \\ []),
    do: Runtime.call(:zrem, opts, &CollectionCommands.zrem(client, key, members, &1))

  @spec zrange(client(), binary(), integer(), integer(), keyword()) ::
          {:ok, [binary() | {binary(), float()}]} | {:error, term()}
  def zrange(client, key, start, stop, opts \\ []),
    do: Runtime.call(:zrange, opts, &ScalarCommands.zrange(client, key, start, stop, &1))

  @spec zscore(client(), binary(), binary(), keyword()) ::
          {:ok, float() | nil} | {:error, term()}
  def zscore(client, key, member, opts \\ []),
    do: Runtime.call(:zscore, opts, &ScalarCommands.zscore(client, key, member, &1))
end
