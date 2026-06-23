defmodule FerricStore do
  @moduledoc """
  Elixir SDK for FerricStore and FerricFlow over the native `ferric://` protocol.
  """

  alias FerricStore.Client

  defdelegate start_link(opts \\ []), to: Client
  defdelegate connect!(opts \\ []), to: Client
  defdelegate close(client), to: Client
  defdelegate pipeline(client, commands, opts \\ []), to: Client

  def command(client, command, args \\ [], opts \\ []) do
    Client.command(client, command, args, opts)
  end

  def ping(client), do: command(client, "PING")
  def get(client, key), do: command(client, "GET", [key])

  def set(client, key, value, opts \\ []) do
    response = command(client, "SET", [key, value] ++ set_opts(opts))
    if response == "OK", do: :ok, else: response
  end

  def delete(client, keys) when is_list(keys), do: command(client, "DEL", keys)
  def delete(client, key), do: delete(client, [key])
  def mget(client, keys), do: command(client, "MGET", keys)

  def mset(client, pairs) when is_map(pairs) do
    args = Enum.flat_map(pairs, fn {key, value} -> [key, value] end)
    command(client, "MSET", args)
  end

  def hset(client, key, field, value), do: command(client, "HSET", [key, field, value])
  def hget(client, key, field), do: command(client, "HGET", [key, field])
  def hmget(client, key, fields), do: command(client, "HMGET", [key | fields])
  def hgetall(client, key), do: command(client, "HGETALL", [key])

  def lpush(client, key, values), do: command(client, "LPUSH", [key | List.wrap(values)])
  def rpush(client, key, values), do: command(client, "RPUSH", [key | List.wrap(values)])
  def lpop(client, key), do: command(client, "LPOP", [key])
  def rpop(client, key), do: command(client, "RPOP", [key])
  def lrange(client, key, start, stop), do: command(client, "LRANGE", [key, start, stop])

  def sadd(client, key, members), do: command(client, "SADD", [key | List.wrap(members)])
  def srem(client, key, members), do: command(client, "SREM", [key | List.wrap(members)])
  def smembers(client, key), do: command(client, "SMEMBERS", [key])
  def sismember(client, key, member), do: command(client, "SISMEMBER", [key, member])

  def zadd(client, key, score, member), do: command(client, "ZADD", [key, score, member])
  def zrem(client, key, members), do: command(client, "ZREM", [key | List.wrap(members)])

  def zrange(client, key, start, stop, opts \\ []),
    do: command(client, "ZRANGE", [key, start, stop] ++ zrange_opts(opts))

  def zscore(client, key, member), do: command(client, "ZSCORE", [key, member])

  defp set_opts(opts) do
    []
    |> append_opt("EX", Keyword.get(opts, :ex))
    |> append_opt("PX", Keyword.get(opts, :px))
    |> append_flag("NX", Keyword.get(opts, :nx))
    |> append_flag("XX", Keyword.get(opts, :xx))
  end

  defp zrange_opts(opts) do
    []
    |> append_flag("WITHSCORES", Keyword.get(opts, :with_scores))
    |> append_flag("REV", Keyword.get(opts, :rev))
  end

  defp append_opt(args, _name, nil), do: args
  defp append_opt(args, name, value), do: args ++ [name, value]
  defp append_flag(args, _name, nil), do: args
  defp append_flag(args, _name, false), do: args
  defp append_flag(args, name, true), do: args ++ [name]
end
