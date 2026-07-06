# Client API

The SDK has one native protocol client and four convenience layers.

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")
```

## `FerricStore`

General client and KV/data-structure helpers.

| Area | Functions |
| --- | --- |
| Lifecycle | `start_link/1`, `connect!/1`, `close/1` |
| Native control | `command/4`, `pipeline/3`, `async_pipeline/3`, `async_native/4`, `await/2`, `yield/2` |
| KV | `get/2`, `set/4`, `delete/2`, `mget/2`, `mset/2` |
| Hash | `hset/4`, `hget/3`, `hmget/3`, `hgetall/2` |
| List | `lpush/3`, `rpush/3`, `lpop/2`, `rpop/2`, `lrange/4` |
| Set | `sadd/3`, `srem/3`, `smembers/2`, `sismember/3` |
| Sorted set | `zadd/4`, `zrem/3`, `zrange/5`, `zscore/3` |
| Management | `FerricStore.SDK.capabilities/2`, ACL, namespace, quota, and telemetry delegates |

Examples:

```elixir
:ok = FerricStore.set(client, "k", "v", ttl_ms: 60_000)
"v" = FerricStore.get(client, "k")

"OK" = FerricStore.mset(client, %{"a" => "1", "b" => "2"})
["1", "2", nil] = FerricStore.mget(client, ["a", "b", "missing"])
```

## `FerricStore.Flow`

Exact FerricFlow command wrapper.

| Area | Functions |
| --- | --- |
| Create | `create/3`, `enqueue/3`, `create_many/3` |
| Read | `get/3`, `list/2`, `history/3` |
| Claim/lease | `claim_due/3` |
| Mutate | `transition/3`, `complete/3`, `complete_many/3`, `retry/3`, `fail/3`, `cancel/3`, `signal/3` |
| Values | `value_put/3`, `value_mget/3` |

Example:

```elixir
FerricStore.Flow.create(client, "flow-1",
  type: "order",
  state: "created",
  payload: "payload",
  attributes: %{tenant: "acme"}
)

[job | _] = FerricStore.Flow.claim_due(client, "order",
  state: "created",
  worker: "worker-1",
  limit: 10
)
```

`claim_due/3` returns compact job maps with attributes by default. Pass
`include_attributes: false` for the leanest hot path.

## `FerricStore.Queue`

Small durable queue helper.

```elixir
queue = FerricStore.Queue.new(client, "email",
  state: "queued",
  worker: "email-worker",
  lease_ms: 30_000
)

FerricStore.Queue.enqueue(queue, "email-1", payload: "welcome")
FerricStore.Queue.run_once(queue, fn job -> handle_email(job) end)
```

## `FerricStore.Workflow`

Explicit state-machine helper.

```elixir
workflow = FerricStore.Workflow.new(client, "order", initial_state: "created")
FerricStore.Workflow.start(workflow, "order-1", payload: "payload")
```

Use `Workflow` when you want business states. Use `Flow` directly when you need
exact command control.

## `FerricStore.SDK.Management`

Narrow control-plane helpers over the stable management command contract.

| Area | Functions |
| --- | --- |
| Capabilities | `capabilities/2` |
| ACL | `set_user/4`, `del_user/3`, `get_user/3`, `list_users/2`, `save_acl/2` |
| Namespace | `ensure_namespace/4`, `get_namespace/3`, `list_namespaces/2`, `delete_namespace/3` |
| Quota | `set_quota/4`, `get_quota/3`, `quota_usage/3` |
| Telemetry | `cluster_info/2`, `namespace_usage/3`, `flow_query/3`, `flow_history/4` |

Top-level `FerricStore.SDK` delegates are also available with `acl_*`,
namespace, quota, and telemetry names.

```elixir
{:ok, caps} = FerricStore.SDK.capabilities(client)

if caps["namespace_management"] do
  FerricStore.SDK.ensure_namespace(client, "tenant:acme", flow_count: 100)
end
```

## Low-level command escape hatch

```elixir
FerricStore.command(client, "FLOW.GET", ["flow-1", "PAYLOAD"])
```

Prefer typed helpers when available. Use `command/4` for advanced or newly added
server commands before the SDK grows a dedicated wrapper.
