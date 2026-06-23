# Production Readiness

## Deployment shape

Use producers and workers as separate services.

```text
Phoenix/API/serverless producer -> FerricStore -> supervised worker service
```

Producers create work and return. Workers claim due work and complete,
transition, retry, or fail it.

## Idempotency

Handlers must be idempotent. A worker can crash after an external side effect
but before completing the flow, then another worker can reclaim the lease.

Recommended pattern:

- Use `flow_id` and state name as external idempotency keys.
- Keep side-effect outputs as named values.
- Do not blindly override named values on retry.
- Pass `partition_key`, `lease_token`, and `fencing_token` from the claimed job.

## Leases

Set `lease_ms` above handler p99 plus margin.

```elixir
queue = FerricStore.Queue.new(client, "email",
  worker: "email-worker",
  lease_ms: 60_000
)
```

If work routinely exceeds the lease, split it into smaller states or add a
server-side lease extension flow through `FerricStore.command/4` until the SDK
has a dedicated helper.

## Partition keys

Use partition keys when you need ordering or locality.

```elixir
FerricStore.Flow.create(client, "order-1",
  type: "order",
  partition_key: "tenant-a:order-1"
)
```

If you omit `partition_key`, FerricStore auto-buckets Flow records for spread.
Always pass the returned `partition_key` from claimed jobs into mutation calls.

## Backpressure

FerricStore can reject writes under overload. Treat overload errors as a signal
to slow producers, not as normal application failure.

Producer policy:

- Retry with bounded exponential backoff.
- Cap in-flight creates.
- Prefer batch create paths for bursts.
- Monitor rejection rate.

## Payload hydration caps

Avoid unbounded reads of large values.

```elixir
FerricStore.Flow.get(client, "flow-1",
  payload: true,
  payload_max_bytes: 64 * 1024
)

FerricStore.Flow.value_mget(client, [ref], max_bytes: 64 * 1024)
```

## Graceful shutdown

Worker services should stop claiming new jobs before shutdown and allow current
handlers to finish. If a worker is killed, leases eventually expire and work can
be reclaimed.

## Observability

Track at least:

- create success/error counts
- claim count and empty claim count
- handler latency
- complete/retry/fail counts
- stale lease errors
- overload errors
- value hydration omissions
