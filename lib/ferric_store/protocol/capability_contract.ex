defmodule FerricStore.Protocol.CapabilityContract do
  @moduledoc false

  alias FerricStore.Protocol.CapabilityOptionalFields
  alias FerricStore.Protocol.CommandSpec

  # Current schemas advertised by the server revision pinned by this SDK.
  # Commands omitted here do not have an advertised request schema.
  @required_schemas %{
    "CAS" => ["key", "expected", "value"],
    "CLUSTER.DEMOTE" => ["args"],
    "CLUSTER.FAILOVER" => ["args"],
    "CLUSTER.HEALTH" => [],
    "CLUSTER.JOIN" => ["args"],
    "CLUSTER.KEYSLOT" => ["key"],
    "CLUSTER.LEAVE" => [],
    "CLUSTER.PROMOTE" => ["args"],
    "CLUSTER.ROLE" => [],
    "CLUSTER.SLOTS" => [],
    "CLUSTER.STATS" => [],
    "CLUSTER.STATUS" => [],
    "EXTEND" => ["key", "owner", "ttl_ms"],
    "FERRICSTORE.BLOBGC" => [],
    "FERRICSTORE.CONFIG" => ["args"],
    "FERRICSTORE.HOTNESS" => [],
    "FERRICSTORE.KEY_INFO" => ["key"],
    "FERRICSTORE.METRICS" => [],
    "FETCH_OR_COMPUTE" => ["key", "ttl_ms"],
    "FETCH_OR_COMPUTE_ERROR" => ["key", "token", "message"],
    "FETCH_OR_COMPUTE_RESULT" => ["key", "token", "value", "ttl_ms"],
    "FLOW.ATTRIBUTES" => ["type"],
    "FLOW.ATTRIBUTE_VALUES" => ["type", "attribute"],
    "FLOW.CLAIM_DUE" => ["type"],
    "FLOW.CANCEL" => ["id", "fencing_token"],
    "FLOW.COMPLETE" => ["id", "lease_token", "fencing_token"],
    "FLOW.CREATE" => ["id"],
    "FLOW.CREATE_MANY" => ["items"],
    "FLOW.FAIL" => ["id", "lease_token", "fencing_token"],
    "FLOW.GET" => ["id"],
    "FLOW.HISTORY" => ["id"],
    "FLOW.QUERY" => ["version", "query"],
    "FLOW.POLICY.GET" => ["type"],
    "FLOW.POLICY.SET" => ["type"],
    "FLOW.RETENTION_CLEANUP" => [],
    "FLOW.RETRY" => ["id", "lease_token", "fencing_token"],
    "FLOW.RUN_STEPS_MANY" => ["items", "type", "worker"],
    "FLOW.SCHEDULE.CREATE" => ["id", "target"],
    "FLOW.SCHEDULE.DELETE" => ["id"],
    "FLOW.SCHEDULE.FIRE" => ["id"],
    "FLOW.SCHEDULE.FIRE_DUE" => [],
    "FLOW.SCHEDULE.GET" => ["id"],
    "FLOW.SCHEDULE.LIST" => [],
    "FLOW.SCHEDULE.PAUSE" => ["id"],
    "FLOW.SCHEDULE.RESUME" => ["id"],
    "FLOW.SIGNAL" => ["id", "signal"],
    "FLOW.SPAWN_CHILDREN" => ["id", "children", "partition_key", "group_id", "fencing_token"],
    "FLOW.START_AND_CLAIM" => ["id", "type", "initial_state", "worker"],
    "FLOW.STATS" => ["type"],
    "FLOW.STEP_CONTINUE" => [
      "id",
      "lease_token",
      "from_state",
      "to_state",
      "fencing_token"
    ],
    "FLOW.TRANSITION" => [
      "id",
      "from_state",
      "to_state",
      "lease_token",
      "fencing_token"
    ],
    "FLOW.VALUE.MGET" => ["refs"],
    "FLOW.VALUE.PUT" => ["value"],
    "GET" => ["key"],
    "HGET" => ["key", "field"],
    "HGETALL" => ["key"],
    "HMGET" => ["key", "fields"],
    "HSET" => ["key", "fields"],
    "LOCK" => ["key", "owner", "ttl_ms"],
    "LPOP" => ["key"],
    "LPUSH" => ["key", "values"],
    "LRANGE" => ["key", "start", "stop"],
    "MGET" => ["keys"],
    "RATELIMIT.ADD" => ["key", "window_ms", "max"],
    "RPOP" => ["key"],
    "RPUSH" => ["key", "values"],
    "SADD" => ["key", "members"],
    "SET" => ["key", "value"],
    "SISMEMBER" => ["key", "member"],
    "SMEMBERS" => ["key"],
    "SREM" => ["key", "members"],
    "UNLOCK" => ["key", "owner"],
    "ZADD" => ["key", "items"],
    "ZRANGE" => ["key", "start", "stop"],
    "ZREM" => ["key", "members"],
    "ZSCORE" => ["key", "member"]
  }

  @required_opcodes Enum.map(CommandSpec.all(), &Map.take(&1, [:name, :opcode]))
  @optional_fields Map.merge(
                     CapabilityOptionalFields.Core.all(),
                     CapabilityOptionalFields.Flow.all()
                   )

  if MapSet.new(Map.keys(@required_schemas)) != MapSet.new(Map.keys(@optional_fields)) do
    raise "capability schema required and optional field contracts must cover the same commands"
  end

  @required_schema_fields Map.new(@required_schemas, fn {command, required} ->
                            {command,
                             required ++ Map.fetch!(@optional_fields, command) ++ ["deadline_ms"]}
                          end)

  @spec required_opcodes() :: [%{name: binary(), opcode: non_neg_integer()}]
  def required_opcodes, do: @required_opcodes

  @spec required_schemas() :: %{binary() => [binary()]}
  def required_schemas, do: @required_schemas

  @spec required_schema_fields() :: %{binary() => [binary()]}
  def required_schema_fields, do: @required_schema_fields
end
