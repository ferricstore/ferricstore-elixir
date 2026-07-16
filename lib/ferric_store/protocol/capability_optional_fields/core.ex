defmodule FerricStore.Protocol.CapabilityOptionalFields.Core do
  @moduledoc false

  @fields %{
    "GET" => [],
    "MGET" => [],
    "SET" => ["ttl", "exat", "pxat", "nx", "xx", "get", "keepttl"],
    "CAS" => ["ttl"],
    "LOCK" => [],
    "UNLOCK" => [],
    "EXTEND" => [],
    "RATELIMIT.ADD" => ["count"],
    "FETCH_OR_COMPUTE" => ["hint"],
    "FETCH_OR_COMPUTE_RESULT" => [],
    "FETCH_OR_COMPUTE_ERROR" => [],
    "HSET" => [],
    "HGET" => [],
    "HMGET" => [],
    "HGETALL" => [],
    "LPUSH" => [],
    "RPUSH" => [],
    "LPOP" => ["count"],
    "RPOP" => ["count"],
    "LRANGE" => [],
    "SADD" => [],
    "SREM" => [],
    "SMEMBERS" => [],
    "SISMEMBER" => [],
    "ZADD" => [],
    "ZREM" => [],
    "ZRANGE" => ["withscores"],
    "ZSCORE" => [],
    "CLUSTER.HEALTH" => ["args"],
    "CLUSTER.STATS" => ["args"],
    "CLUSTER.KEYSLOT" => ["args"],
    "CLUSTER.SLOTS" => ["args"],
    "CLUSTER.STATUS" => ["args"],
    "CLUSTER.JOIN" => [],
    "CLUSTER.LEAVE" => ["args"],
    "CLUSTER.FAILOVER" => [],
    "CLUSTER.PROMOTE" => [],
    "CLUSTER.DEMOTE" => [],
    "CLUSTER.ROLE" => ["args"],
    "FERRICSTORE.KEY_INFO" => ["args"],
    "FERRICSTORE.CONFIG" => [],
    "FERRICSTORE.HOTNESS" => ["args"],
    "FERRICSTORE.METRICS" => ["args"],
    "FERRICSTORE.BLOBGC" => ["args"]
  }

  @spec all() :: %{binary() => [binary()]}
  def all, do: @fields
end
