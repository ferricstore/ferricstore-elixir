# FerricStore Elixir SDK

Elixir SDK for FerricStore and FerricFlow over native TCP and stateless HTTP.

Status: public beta. SDK `0.11.9` requires FerricStore `~> 0.11.4` and
negotiates compact Stream mode 34 and compact Pub/Sub mode 35 with FerricStore
0.11.8. Native wire framing and the generic compatibility paths remain protocol
v1. APIs may change before `1.0`, but the SDK is
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
    {:ferricstore_sdk, "~> 0.11.9"}
  ]
end
```

For local SDK development:

```bash
mix deps.get
mix test
```

### 2. Start FerricStore

For local development, run the same immutable FerricStore 0.11.8 image used by
the SDK integration workflow:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  quay.io/ferricstore/ferricstore:0.11.8@sha256:d472b337fcec536b46e4ba7549689bc5c7fc67948071f6e39cc13ca0e8879ce2
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

The same command API can use a FerricStore HTTP server. HTTP/1.1 keeps
connections alive; set `http2: true` for a multiplexed HTTP/2 connection:

```elixir
{:ok, client} =
  FerricStore.SDK.from_url("https://ferricstore-http.example.com",
    username: "default",
    password: password,
    http2: true
  )

{:ok, "PONG"} = FerricStore.SDK.ping(client)
```

Use `bearer_token:` for Bearer authentication. Basic username/password
authentication is accepted only with `https://`; omitting the username uses
`default`. An SDK pipeline becomes one ordered HTTP request. Limits include
`timeout:`, `max_request_bytes:`, `max_response_bytes:`, `max_batch_items:`,
`max_connections:`, and `max_concurrent_requests:`.

For private certificate authorities, configure the shared Finch pools before
the application starts:

```elixir
config :ferricstore_sdk,
  http_pool_transport_options: [verify: :verify_peer, cacertfile: "/etc/ssl/ferricstore-ca.pem"]
```

HTTP requests are stateless. `AUTH`, `CLIENT`, transactions, Pub/Sub
subscriptions, blocking list/stream reads, and `WATCH` require native TCP and
fail locally when used through HTTP. Redirects retain authentication and custom
headers across origins; configure only endpoints and redirect targets you
trust.

### 4. Query durable runs

Use parameterized FQL for bounded, partition-scoped reads. Cursors are opaque
and must be reused with the same query and parameters.

```elixir
query = """
FROM runs
WHERE partition_key = @partition AND type = @type AND state = @state
ORDER BY updated_at_ms DESC
LIMIT 25
RETURN RECORDS
"""

params = %{"partition" => "partition-a", "type" => "invoice", "state" => "queued"}
%FerricStore.Flow.QueryResult{records: records, page: page} =
  FerricStore.Flow.query(client, query, params)

%FerricStore.Flow.QueryExplainResult{} = FerricStore.Flow.explain(client, query, params)
%FerricStore.Flow.QueryIndexStatus{} = FerricStore.Flow.query_indexes(client)
```

Each `%FerricStore.Flow.QueryIndex{}` reports `covering_fields`, including
covered dynamic `attribute.*` and `state_meta.*` paths, and an opaque `format`
describing its derived-storage generation. Use format changes to identify a
rebuild requirement; do not decode server storage from these values. The
counter format is `nil` when an index has no exact count prefix.

Select a sparse result map by adding up to 32 source-specific fields after
`RETURN RECORD` or `RETURN RECORDS`, for example
`RETURN RECORDS (run_id, state, attribute['customer'])`. A bare return keeps the
complete public record. Projection runs after authorization, authoritative
recheck, ordering, and cursor calculation: it reduces retained result data,
encoding, network, and client decoding work, but not index scans or hydration.

Build the return clause with validated source-aware selectors instead of
hand-quoting metadata names:

```elixir
{:ok, projected} =
  FerricStore.Flow.QueryProjection.project(
    "FROM runs WHERE partition_key = @partition AND run_id = @run",
    :record,
    [:run_id, :state, {:attribute, "customer"}]
  )

%FerricStore.Flow.QueryResult{} = FerricStore.Flow.query(client, projected, params)
```

Collection helpers accept the same run selectors through `fields:` and compile
the projection into FQL before transport:

```elixir
records =
  FerricStore.Flow.list(client,
    type: "invoice",
    state: "queued",
    partition_key: "tenant-a",
    fields: [:run_id, :state, {:attribute, "customer"}]
  )
```

`fields:` is supported by `list`, `search`, `terminals`, `failures`, `stuck`,
`by_parent`, `by_root`, and `by_correlation`. Omitting it returns complete public
records. An explicit projection must contain 1 to 32 unique, source-valid
selectors.

### 5. Create a durable queue item

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
%FerricStore.Flow.PolicySnapshot{generation: generation} =
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

FIFO state policy is opt-in per state. Use an explicit partition key for records
that enter FIFO states; priority ordering is a parallel-state feature and the
server rejects priority on FIFO entries:

```elixir
FerricStore.Flow.policy_set(client, "order",
  states: %{"created" => [mode: :fifo]}
)

FerricStore.Flow.create(client, "order-3",
  type: "order",
  state: "created",
  partition_key: "tenant-a:order-3",
  payload: "payload"
)
```

Direct policy updates deep-patch by default. Use `replace: true` to reset
omitted fields, or compare-and-swap against a snapshot generation:

```elixir
FerricStore.Flow.policy_set(client, "order",
  expected_generation: generation,
  states: %{"created" => [mode: :fifo]}
)
```

Stale generations return `FerricStore.Flow.StalePolicyGenerationError` and are
never retried automatically. `Workflow.install_policy/2` uses full replacement
by default because a workflow declaration is a complete policy snapshot.

Use `FerricStore.SDK` when you want explicit `{:ok, value}` results and the
complete topology-aware API surface:

```elixir
{:ok, sdk} = FerricStore.SDK.start_link(url: "ferric://127.0.0.1:6388")
{:ok, :ok} = FerricStore.SDK.set(sdk, "{tenant:1}:hello", "world")
{:ok, "world"} = FerricStore.SDK.get(sdk, "{tenant:1}:hello")
```

`FerricStore.start_link/1` and `FerricStore.SDK.start_link/1` return the same
topology-aware client type. It can be shared across `FerricStore`,
`FerricStore.Flow`, `Queue`, `Workflow`, and every `FerricStore.SDK` namespace.

### 10. Probe management capabilities

Control-plane callers should probe capabilities before enabling management UI
or automation:

```elixir
{:ok, caps} = FerricStore.SDK.capabilities(sdk)

if caps["acl_management"] do
  FerricStore.SDK.acl_set_user(sdk, "platform_worker_abcd", [
    "on",
    ">secret",
    "+PING",
    "+@read",
    "+@write",
    "-@dangerous",
    "-@admin",
    "~tenant:namespace:*"
  ])
end
```

The SDK also exposes narrow namespace, quota, and safe telemetry helpers through
`FerricStore.SDK.Management` and top-level `FerricStore.SDK` delegates.

### 11. Enterprise invocation helpers

FerricStore Enterprise exposes invocation definitions and invocation creation
through the same native SDK client:

```elixir
{:ok, sdk} = FerricStore.SDK.start_link(url: "ferric://127.0.0.1:6388")

{:ok, definition} =
  FerricStore.SDK.invocation_definition_put(sdk, %{
    name: "send-email",
    acl: %{scope_required: true},
    partition: %{key: "tenant:{tenant}:invocation:send-email"}
  })

{:ok, created} =
  FerricStore.SDK.invocation_create(sdk, "send-email", %{tenant: "acme"},
    context: %{subject: "user-1"}
  )

{:ok, invocation} = FerricStore.SDK.invocation_get(sdk, created["invocation_id"])
{:ok, partitions} = FerricStore.SDK.invocation_partition_list(sdk, "send-email")
```

Trusted proxy deployments can pass `request_context: %{...}`. The context is
sent out-of-band through the native command envelope, so untrusted callers cannot
spoof it by only editing the invocation payload.

## What you use

- `FerricStore` for native protocol connection and KV/data-structure helpers.
- `FerricStore.SDK` for topology-aware routing and native command wrappers.
- `FerricStore.SDK.Flow`, `FerricStore.SDK.Admin`, and `FerricStore.SDK.Management`
  for typed-map Flow, admin, and control-plane command surfaces.
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
multiplexed native socket per SDK client process. HTTP/1.1 uses a bounded shared
keep-alive pool and HTTP/2 multiplexes requests over a shared connection; create
more clients only after profiling shows client-side saturation.

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
scripts/test_integration.sh
```
