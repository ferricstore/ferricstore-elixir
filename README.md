# FerricStore Elixir SDK

Elixir SDK for FerricStore and FerricFlow over the native `ferric://` protocol.

Status: public alpha `0.2.0`. APIs may change before `1.0`, but the SDK is
covered by command-construction tests, architecture tests, Docker-backed
integration tests, and local benchmark scripts.

FerricFlow keeps each workflow or job's state and history in one durable place.
It is an explicit durable state pipeline, not a hidden deterministic replay
engine:

```text
create -> claim -> handler -> transition/complete/retry/fail
```

Handlers should be idempotent because work can be retried after lease expiry,
worker crash, or explicit retry.

Durability is the default contract. A workflow command returns success only
after the state change is accepted by FerricStore and written through its durable
path.

## First 10 minutes

### 1. Install

```elixir
def deps do
  [
    {:ferricstore_sdk, "~> 0.2.0"}
  ]
end
```

For local SDK development:

```bash
mix deps.get
mix test
```

### 2. Start FerricStore

For local development, use the Docker image:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  ghcr.io/ferricstore/ferricstore:0.7.1
```

The SDK examples assume:

```text
ferric://127.0.0.1:6388
```

### 3. Connect

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")

:ok = FerricStore.set(client, "hello", "world")
"world" = FerricStore.get(client, "hello")
```

### 4. Create a durable queue item

```elixir
queue = FerricStore.Queue.new(client, "email", worker: "email-worker")

FerricStore.Queue.enqueue(queue, "email-1",
  payload: "welcome:user-1",
  attributes: %{tenant: "acme", campaign: "summer"}
)
```

Attributes are small indexed metadata. They are useful for search, filtering,
and debugging. They are not payload bytes.

### 5. Process one queue batch

```elixir
FerricStore.Queue.run_once(queue, fn job ->
  send_email(job["payload"])
  "sent"
end)
```

`run_once/3` claims due work and completes or fails the job based on the handler
result. For a long-running worker, call it from a supervised process with your
own shutdown and concurrency policy.

### 6. Create a workflow/state machine

Use workflows when one durable flow moves through named states.

```elixir
workflow = FerricStore.Workflow.new(client, "order", initial_state: "created")

FerricStore.Workflow.start(workflow, "order-1",
  payload: "order payload",
  attributes: %{tenant: "acme"},
  values: %{order: :erlang.term_to_binary(%{total: 120})}
)
```

Claim, transition, and complete explicitly:

```elixir
[job | _] = FerricStore.Workflow.claim(workflow, "created", limit: 1)

FerricStore.Workflow.transition(workflow, job["id"], "running", "charged",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  payload: "charged"
)

[job | _] = FerricStore.Workflow.claim(workflow, "charged", limit: 1)

FerricStore.Workflow.complete(workflow, job["id"],
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  result: "ok"
)
```

After `claim_due`, the current durable state is `running`; the original claimed
state is tracked as run state. Pass `from_state: "running"` when transitioning a
claimed job.

### 7. Store and fetch named values

Use named values/value refs when different states need different pieces of data.
Values are only hydrated when requested.

```elixir
meta = FerricStore.Flow.value_put(client, "large invoice bytes",
  owner_flow_id: "order-1",
  name: "invoice_pdf",
  override: false
)

ref = meta["ref"]
["large invoice bytes"] = FerricStore.Flow.value_mget(client, [ref])
```

Keep `override: false` for normal first-write values. Use `override: true` only
when replacing a value is intentional.

### 8. Inspect state and history

```elixir
record = FerricStore.Flow.get(client, "order-1", payload: true)
history = FerricStore.Flow.history(client, "order-1")
```

History is for debugging and audit. Handlers should use claimed job data and
requested values, not history replay.

### 9. Index one state metadata key

State metadata is stored per flow state. A flow type may choose one state
metadata key for server-side indexing:

```elixir
FerricStore.Flow.policy_set(client, "order", indexed_state_meta: "version")

FerricStore.Flow.create(client, "order-2",
  type: "order",
  state: "accept",
  state_meta: %{version: 1, owner: "risk"}
)

FerricStore.Flow.search(client,
  type: "order",
  state: "accept",
  state_meta: %{version: 1},
  count: 10
)
```

Use `FerricStore.SDK` when you want topology-aware routing from the client:

```elixir
{:ok, sdk} = FerricStore.SDK.start_link(url: "ferric://127.0.0.1:6388")
:ok = FerricStore.SDK.set(sdk, "{tenant:1}:hello", "world")
{:ok, "world"} = FerricStore.SDK.get(sdk, "{tenant:1}:hello")
```

## What you use

- `FerricStore` for native protocol connection and KV/data-structure helpers.
- `FerricStore.SDK` for topology-aware routing and native command wrappers.
- `FerricStore.Flow` for exact FerricFlow command-level control.
- `FerricStore.Queue` for simple durable queue helpers.
- `FerricStore.Workflow` for explicit state-machine helpers.
- `FerricStore.Codec.Raw` by default.
- `FerricStore.Codec.Term` for Elixir-only term payloads.
- `FerricStore.command/4` as the low-level command escape hatch.

## Production shape

Use one process/service to create work and a separate long-lived worker service
to claim and complete work.

```text
Phoenix/API/serverless producer -> FerricStore -> supervised worker service
```

Before production, configure timeouts, lease duration, backpressure behavior,
graceful shutdown, and value hydration caps. The `ferric://` transport uses one
multiplexed native socket per SDK client process; create more client processes
only after profiling shows client-side saturation.

## Docs

- [Documentation index](docs/index.md)
- [Quickstart](docs/quickstart.md)
- [Client API](docs/client.md)
- [Workflow and queue APIs](docs/workflow.md)
- [Data, attributes, and value refs](docs/data.md)
- [Configuration](docs/configuration.md)
- [Production readiness](docs/production.md)
- [Use cases](docs/use-cases.md)
- [Web and serverless usage](docs/web.md)
- [Testing](docs/testing.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Benchmark notes](docs/benchmark.md)
- [Development checks](docs/development.md)

## Integration tests

Integration tests are explicit ExUnit integration tests. They run against the
same Docker image used by CI:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  ghcr.io/ferricstore/ferricstore:0.7.1

mix test --only integration
```
