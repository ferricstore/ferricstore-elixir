# Client API

The SDK exposes one topology-aware native client through two interchangeable
facades:

- `FerricStore.start_link/1` provides concise unwrapped success values for
  `FerricStore`, `FerricStore.Flow`, `FerricStore.Queue`, and
  `FerricStore.Workflow`.
- `FerricStore.SDK.start_link/1` exposes explicit `{:ok, value}` results for the
  `KV`, `Flow`, `Admin`, `Management`, and `Invocation` helpers.

Both functions return `FerricStore.SDK.Native.Client`; one client can be shared
across every namespace. The runtime client is a pid; registered-name and
`{:via, ...}` client identifiers are not supported.

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")
```

All three public entry points (`FerricStore`, `FerricStore.Client`, and
`FerricStore.SDK`) expose `child_spec/1`, so applications can place the client
directly in a supervision tree:

```elixir
children = [
  {FerricStore, url: "ferric://127.0.0.1:6388"}
]

Supervisor.start_link(children, strategy: :one_for_one)
```

## `FerricStore`

General client and KV/data-structure helpers.

| Area | Functions |
| --- | --- |
| Lifecycle | `start_link/1`, `connect!/1`, `close/1` |
| Native control | `command/4`, `pipeline/3`, `async_pipeline/3`, `async_native/4`, `await/2`, `yield/2`, `cancel_async/1` |
| KV | `get/2`, `set/4`, `delete/2`, `mget/2`, `mset/2` |
| Hash | `hset/4`, `hget/3`, `hmget/3`, `hgetall/2` |
| List | `lpush/3`, `rpush/3`, `lpop/2`, `rpop/2`, `lrange/4` |
| Set | `sadd/3`, `srem/3`, `smembers/2`, `sismember/3` |
| Sorted set | `zadd/4`, `zrem/3`, `zrange/5`, `zscore/3` |

Examples:

```elixir
:ok = FerricStore.set(client, "k", "v", ttl: 60_000)
"v" = FerricStore.get(client, "k")

:ok = FerricStore.mset(client, %{"a" => "1", "b" => "2"})
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
| Policy | `policy_set/3`, `policy_get/3` |

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

FIFO Flow state policy is opt-in per state:

```elixir
FerricStore.Flow.policy_set(client, "order",
  states: %{"created" => [mode: :fifo]}
)
```

Records that enter FIFO states must use `partition_key`. Do not set `priority`
on FIFO records or transitions; the server rejects priority for FIFO states.

Policy reads and writes return `%FerricStore.Flow.PolicySnapshot{}` with a
monotonic `generation`. Direct writes deep-patch by default; pass
`replace: true` for full replacement or `expected_generation: generation` for
compare-and-swap. A stale CAS returns
`FerricStore.Flow.StalePolicyGenerationError` and is never retried.

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
{:ok, sdk} = FerricStore.SDK.start_link(url: "ferric://127.0.0.1:6388")
{:ok, caps} = FerricStore.SDK.capabilities(sdk)

if caps["namespace_management"] do
  FerricStore.SDK.ensure_namespace(sdk, "tenant:acme", flow_count: 100)
end
```

## Low-level command escape hatch

```elixir
FerricStore.command(client, "FLOW.GET", ["flow-1", "PAYLOAD"])
```

Prefer typed helpers when available. Use `command/4` for advanced or newly added
server commands before the SDK grows a dedicated wrapper.

The high-level facade unwraps successful values. The `FerricStore.SDK` facade,
including `FerricStore.SDK.command/4`, consistently returns explicit
`{:ok, value}` or `{:error, reason}` tuples.

High-level failures consistently return `{:error, %FerricStore.Error{}}`; the
original SDK reason remains available in `error.raw`. SDK namespaces keep raw
error reasons for callers that need protocol-level matching. Successful SDK
writes use `{:ok, :ok}` rather than a bare `:ok`.
