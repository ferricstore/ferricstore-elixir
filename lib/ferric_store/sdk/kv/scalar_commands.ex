defmodule FerricStore.SDK.KV.ScalarCommands do
  @moduledoc false

  alias FerricStore.SDK.KV.{
    CollectionReadCommands,
    ComputeCommands,
    HashReadCommands,
    LeaseCommands,
    SortedSetReadCommands,
    StringCommands
  }

  defdelegate get(client, key, opts), to: StringCommands
  defdelegate set(client, key, value, opts), to: StringCommands
  defdelegate cas(client, key, expected, value, opts), to: StringCommands
  defdelegate lock(client, key, owner, ttl_ms, opts), to: LeaseCommands
  defdelegate unlock(client, key, owner, opts), to: LeaseCommands
  defdelegate extend(client, key, owner, ttl_ms, opts), to: LeaseCommands
  defdelegate ratelimit_add(client, key, window_ms, max, count, opts), to: ComputeCommands
  defdelegate fetch_or_compute(client, key, ttl_ms, opts), to: ComputeCommands

  defdelegate fetch_or_compute_result(client, key, token, value, ttl_ms, opts),
    to: ComputeCommands

  defdelegate fetch_or_compute_error(client, key, token, message, opts), to: ComputeCommands
  defdelegate hget(client, key, field, opts), to: HashReadCommands
  defdelegate hgetall(client, key, opts), to: HashReadCommands
  defdelegate lpop(client, key, count, opts), to: CollectionReadCommands
  defdelegate rpop(client, key, count, opts), to: CollectionReadCommands
  defdelegate lrange(client, key, start, stop, opts), to: CollectionReadCommands
  defdelegate smembers(client, key, opts), to: CollectionReadCommands
  defdelegate sismember(client, key, member, opts), to: CollectionReadCommands
  defdelegate zrange(client, key, start, stop, opts), to: SortedSetReadCommands
  defdelegate zscore(client, key, member, opts), to: SortedSetReadCommands
end
