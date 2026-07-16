# Configuration

## URL

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")
```

Supported schemes:

| Scheme | Use |
| --- | --- |
| `ferric://` | Plain TCP, local/dev/private network. |
| `ferrics://` | TLS TCP. |
| `ferric+tls://` | TLS TCP alias for topology-aware clients. |

When the port is omitted, the SDK uses `6388` for plain TCP and `6389` for
TLS. Percent-encoded usernames and passwords are decoded before `AUTH`.

Credentials can be passed in the URL when server ACL/auth is enabled:

```elixir
{:ok, client} = FerricStore.start_link(
  url: "ferrics://app_user:secret@ferricstore.service:6389"
)
```

## Timeouts

Client calls accept `timeout` where the SDK exposes request options:

```elixir
FerricStore.Flow.claim_due(client, "email",
  state: "queued",
  worker: "worker-1",
  limit: 100,
  timeout: 10_000
)
```

Use larger timeouts for commands that intentionally wait, such as blocking
claims. Data requests also carry an absolute `deadline_ms`, so work that has
already timed out at the caller is not left running indefinitely on the server.
Finite request and connection timers must be between `0` and `4294967295`
milliseconds (`heartbeat_interval` and `topology_refresh_timeout` must be
positive). Use `:infinity` where the option explicitly supports an unbounded
timeout; invalid values fail locally instead of reaching a timer process.

Socket writes have a separate finite `send_timeout` (default `5000` ms). Set it
to a non-negative integer when a deployment needs a different bound;
`:infinity` is intentionally rejected. If a peer stops reading, the timed-out
write closes that socket so queued requests fail or retry through the normal
transport policy instead of leaving an encoder blocked indefinitely.

## Multiplexing

One topology coordinator owns a bounded set of native connections and
multiplexes requests by request id. Connections are keyed by endpoint, TLS
identity, and operational limits, so incompatible policies are never mixed.

```elixir
{:ok, client} = FerricStore.start_link(url: url)
```

Create more client processes only after profiling shows the coordinator or
connection set is the bottleneck.

Admission and request/response memory are bounded. The client accepts
`max_request_bytes` (default 16 MiB), `max_frame_bytes`,
`max_response_bytes`, per-connection `max_in_flight`, and coordinator-wide
`max_pending_requests`. Oversized requests are
rejected before the encoded body is flattened or sent. Requests above an
admission limit fail immediately with a backpressure error instead of waiting
past their deadline.

`max_response_bytes` limits one logical response. `max_response_buffer_bytes`
limits the combined bytes retained by all incomplete responses on a socket and
defaults to `max_response_bytes`. Multi-shard calls use
`max_group_concurrency` (default `32`) for both connection setup and requests.
`max_batch_items` (default `100000`) bounds one batch, `max_connecting`
(default `32`) bounds simultaneous establishment, and `max_connections`
(default `64`) caps active, retiring, and connecting sessions across all
endpoint policy profiles. `connections_per_endpoint` (default `2`) bounds the
session pool for one endpoint. A second session is opened on demand when every
existing session is busy, avoiding encoder and socket head-of-line blocking
without adding an idle connection for sequential workloads. Capacity exhaustion
returns `:connection_backpressure`. A timed-out attempt with no remaining
waiters is cancelled together with its half-open connection.

Pipelines are admitted twice: a client safety ceiling prevents unbounded local
preprocessing, then the selected connection enforces the server-advertised
`max_pipeline_commands` before constructing the wire payload.

Native typed and compact response collections are capped at 100000 decoded
items. This also covers count-only compact responses, whose declared item count
is not otherwise constrained by response bytes.

Unsolicited chunked server frames are bounded independently on every connection.
Use `max_server_chunk_streams`, `max_server_chunk_bytes`, and
`server_chunk_timeout` to tighten the defaults for untrusted networks. The
defaults allow 64 incomplete streams, cap their combined payload at
`max_response_bytes`, and expire them after 30 seconds.

## Server events and draining

The topology-aware client can subscribe to request-id-zero server events:

```elixir
{:ok, _} = FerricStore.SDK.subscribe_events(client, ["FLOW_WAKE"])

case FerricStore.SDK.await_event(client, 5_000) do
  {:ok, %{name: "EVENT", value: value}} -> handle_event(value)
  nil -> :timeout
end
```

Event messages are tagged as `{:ferricstore_event, client, event}`. Passing the
client to `await_event/2` lets one subscriber safely consume events from several
clients without confusing their sources.

`GOAWAY` is delivered to subscribers, drains the affected connection, and the
client automatically establishes a fresh connection and replays the active
event set. `drain_timeout` (default 5 seconds) bounds how long in-flight work can
keep a retiring socket alive; requests still pending at that deadline fail with
`:connection_drained`. Retiring sockets continue to consume `max_connections`
capacity until they exit. Subscriptions are reference-counted across local
subscriber processes. Use `unsubscribe_events/3` when a subscriber no longer
needs events; subscriber processes are also monitored and removed automatically
when they exit.

Native connections send an idle heartbeat every 30 seconds. Configure
`heartbeat_interval` and `heartbeat_timeout`, or set the interval to
`:infinity` to disable heartbeats. Topology refreshes have one total
`topology_refresh_timeout` (default 5 seconds) and examine at most
`max_refresh_candidates` endpoints (default 32). The same total deadline covers
initial STARTUP/AUTH/SHARDS bootstrap and endpoint validation. User-provided
`endpoint_validator` callbacks run outside the client coordinator, so a slow or
failing validator cannot stall or terminate unrelated client traffic.

`endpoint_policy` defaults to `:seed_hosts`. It permits learned endpoints on a
configured seed or `trusted_hosts` host. Use `:none` to permit only exact seed
transport identities, `{:allow_hosts, hosts}` for an explicit host list, or
`:any` only on a fully trusted network. Host collections must be proper lists;
scalar compatibility forms are rejected. Configured seeds must fit within
`max_refresh_candidates`, so no fallback is silently ignored.

## Lanes

Advanced calls can pass `lane_id`:

```elixir
request =
  FerricStore.async_native(
    client,
    FerricStore.Protocol.opcode(:get),
    %{"key" => "k"},
    lane_id: 2
  )

"value" = FerricStore.await(request)
```

Async calls return an owner-bound `FerricStore.AsyncRequest`. Awaiting from a
different process is rejected, an await timeout cancels the underlying request,
and `FerricStore.cancel_async/1` provides explicit cancellation. Submission is
synchronously admitted by the coordinator before the handle is returned, so a
producer cannot create an unbounded cast backlog. Most
application code should ignore lanes and let the SDK default work.

## TLS verification

For `ferrics://`, the SDK uses Erlang `:ssl` with peer verification enabled by
default. For local development against a test listener, verification can be
disabled:

```elixir
{:ok, client} = FerricStore.start_link(
  url: "ferrics://ferricstore.service:6389",
  verify: false
)
```

Keep verification enabled in production. Hostname verification and SNI use the
URL host by default. Private PKI deployments can pass `cacertfile` or `cacerts`;
pass `server_name` when the advertised IP or hostname differs from the
certificate identity.

## Codecs

Use `FerricStore.Codec.Raw` for maximum simplicity and cross-language bytes.
Use `FerricStore.Codec.Term` only when all producers/workers are Elixir.

```elixir
queue = FerricStore.Queue.new(client, "email", codec: FerricStore.Codec.Raw)
```
