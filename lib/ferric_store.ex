defmodule FerricStore do
  @moduledoc """
  High-level FerricStore and FerricFlow API over the topology-aware native client.

  `FerricStore.start_link/1` and `FerricStore.SDK.start_link/1` return the same
  client type. This facade unwraps successful SDK result tuples for concise
  application code; `FerricStore.SDK` retains explicit `{:ok, value}` results.
  """

  alias FerricStore.Client
  alias FerricStore.Compatibility
  alias FerricStore.Result
  alias FerricStore.SDK

  @spec child_spec(keyword() | binary()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  defdelegate start_link(opts \\ []), to: Client
  defdelegate connect!(opts \\ []), to: Client
  defdelegate close(client), to: Client
  defdelegate close(client, timeout), to: Client
  defdelegate pipeline(client, commands, opts \\ []), to: Client
  defdelegate async_pipeline(client, commands, opts \\ []), to: Client
  defdelegate async_native(client, opcode, payload, opts \\ []), to: Client
  defdelegate await(request, timeout \\ 5_000), to: Client
  defdelegate yield(request, timeout \\ 0), to: Client
  defdelegate cancel_async(request), to: Client
  defdelegate minimum_server_version(), to: Compatibility

  def command(client, command, args \\ [], opts \\ []) do
    Client.command(client, command, args, opts)
  end

  def ping(client), do: command(client, "PING")

  def get(client, key), do: client |> SDK.get(key) |> Result.unwrap()

  def set(client, key, value, opts \\ []),
    do: client |> SDK.set(key, value, opts) |> Result.unwrap()

  def delete(client, key_or_keys, opts \\ [])

  def delete(client, keys, opts) when is_list(keys),
    do: client |> SDK.del(keys, opts) |> Result.unwrap()

  def delete(client, key, opts), do: delete(client, [key], opts)

  def mget(client, keys), do: client |> SDK.mget(keys) |> Result.unwrap()

  def mset(client, pairs, opts \\ []),
    do: client |> SDK.mset(pairs, opts) |> Result.unwrap()

  def msetnx(client, pairs, opts \\ []),
    do: client |> SDK.msetnx(pairs, opts) |> Result.unwrap()

  def hset(client, key, field, value),
    do: client |> SDK.hset(key, %{field => value}) |> Result.unwrap()

  def hget(client, key, field), do: client |> SDK.hget(key, field) |> Result.unwrap()
  def hmget(client, key, fields), do: client |> SDK.hmget(key, fields) |> Result.unwrap()
  def hgetall(client, key), do: client |> SDK.hgetall(key) |> Result.unwrap()

  def lpush(client, key, values), do: client |> SDK.lpush(key, values) |> Result.unwrap()
  def rpush(client, key, values), do: client |> SDK.rpush(key, values) |> Result.unwrap()
  def lpop(client, key), do: client |> SDK.lpop(key) |> Result.unwrap()
  def rpop(client, key), do: client |> SDK.rpop(key) |> Result.unwrap()

  def lrange(client, key, start, stop),
    do: client |> SDK.lrange(key, start, stop) |> Result.unwrap()

  def sadd(client, key, members), do: client |> SDK.sadd(key, members) |> Result.unwrap()
  def srem(client, key, members), do: client |> SDK.srem(key, members) |> Result.unwrap()
  def smembers(client, key), do: client |> SDK.smembers(key) |> Result.unwrap()
  def sismember(client, key, member), do: client |> SDK.sismember(key, member) |> Result.unwrap()

  def zadd(client, key, score, member),
    do: client |> SDK.zadd(key, [{score, member}]) |> Result.unwrap()

  def zrem(client, key, members), do: client |> SDK.zrem(key, members) |> Result.unwrap()

  def zrange(client, key, start, stop, opts \\ []),
    do: client |> SDK.zrange(key, start, stop, opts) |> Result.unwrap()

  def zscore(client, key, member), do: client |> SDK.zscore(key, member) |> Result.unwrap()
end
