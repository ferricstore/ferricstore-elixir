defmodule FerricStore do
  @moduledoc """
  Elixir SDK for FerricStore and FerricFlow over the native `ferric://` protocol.
  """

  alias FerricStore.Client

  defdelegate start_link(opts \\ []), to: Client
  defdelegate connect!(opts \\ []), to: Client
  defdelegate close(client), to: Client
  defdelegate pipeline(client, commands, opts \\ []), to: Client
  defdelegate async_pipeline(client, commands, opts \\ []), to: Client
  defdelegate async_native(client, opcode, payload, opts \\ []), to: Client
  defdelegate await(ref, timeout \\ 5_000), to: Client
  defdelegate yield(ref, timeout \\ 0), to: Client

  def command(client, command, args \\ [], opts \\ []) do
    Client.command(client, command, args, opts)
  end

  def ping(client), do: command(client, "PING")

  def get(client, key),
    do: Client.native(client, FerricStore.Protocol.opcode(:get), %{"key" => key})

  def set(client, key, value, opts \\ []) do
    response =
      Client.native(
        client,
        FerricStore.Protocol.opcode(:set),
        %{"key" => key, "value" => value} |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
      )

    if response == "OK", do: :ok, else: response
  end

  def delete(client, keys) when is_list(keys), do: command(client, "DEL", keys)
  def delete(client, key), do: delete(client, [key])

  def mget(client, keys),
    do: Client.native(client, FerricStore.Protocol.opcode(:mget), %{"keys" => keys})

  def mset(client, pairs) when is_map(pairs), do: mset(client, Map.to_list(pairs))

  def mset(client, pairs) when is_list(pairs) do
    Client.native(client, FerricStore.Protocol.opcode(:mset), %{
      "pairs" => Enum.map(pairs, &kv_pair/1)
    })
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

  defp zrange_opts(opts) do
    []
    |> append_flag("WITHSCORES", Keyword.get(opts, :with_scores))
    |> append_flag("REV", Keyword.get(opts, :rev))
  end

  defp append_flag(args, _name, nil), do: args
  defp append_flag(args, _name, false), do: args
  defp append_flag(args, name, true), do: args ++ [name]
  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp kv_pair({key, value}), do: [key, value]
  defp kv_pair([key, value]), do: [key, value]
end
