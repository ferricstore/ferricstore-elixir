# Troubleshooting

## Connection closes immediately

Likely causes:

- FerricStore native listener is not ready yet.
- Wrong port.
- Using `ferrics://` against a plaintext listener or the reverse.

Fix:

```elixir
client = FerricStore.connect!(url: "ferric://127.0.0.1:6388")
FerricStore.ping(client)
```

In CI, wait with SDK `PING`, not just `nc`, because TCP can open before all
shards are ready.

## `ERR syntax error`

Usually the command shape is wrong.

Common Flow examples:

- `FLOW.GET` payload hydration is a flag: `PAYLOAD`, not `PAYLOAD true`.
- `FLOW.LIST` takes type positionally.
- `FLOW.CANCEL` uses `FENCING` and `REASON`, not `LEASE_TOKEN`.

Prefer SDK helpers when available.

## `ERR flow wrong state`

After a job is claimed, current state is `running`. Use `from_state: "running"`
when transitioning claimed work.

```elixir
FerricStore.Workflow.transition(workflow, job["id"], "running", "next_state",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"]
)
```

## Stale lease

The worker no longer owns the flow.

Fix:

- Do not reuse old job maps.
- Keep handlers under `lease_ms`.
- Always pass `lease_token`, `fencing_token`, and `partition_key` from the job.
- Split long work into multiple states.

## Duplicate flow id

FerricStore flow ids are durable. If a local Docker container keeps data between
runs, reused ids can collide.

Use unique ids in tests:

```elixir
id = "flow-#{System.system_time(:nanosecond)}"
```

For producer retries, design ids to be deterministic and idempotent at the
application layer.

## Large value omitted or rejected

Increase the explicit hydration cap only where needed:

```elixir
FerricStore.Flow.value_mget(client, [ref], max_bytes: 512 * 1024)
```

Do not hydrate large values by default in hot handlers.

## High empty-claim rate

Likely causes:

- workers polling the wrong type/state
- too many workers for current backlog
- delayed work not due yet
- missing partition key filters in custom code

Fix:

- Claim explicit states.
- Increase idle sleep for low-volume queues.
- Use larger claim limits for hot queues.
- Keep producer and worker type/state names consistent.
