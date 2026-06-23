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
claims.

## Multiplexing

One `FerricStore.Client` process owns one native socket and multiplexes requests
by request id. This is the default and is the right starting point.

```elixir
{:ok, client} = FerricStore.start_link(url: url)
```

Create more client processes only after profiling shows the client process or
socket is the bottleneck.

## Lanes

Advanced calls can pass `lane_id`:

```elixir
FerricStore.async_native(client, FerricStore.Protocol.opcode(:get), %{"key" => "k"}, lane_id: 2)
```

Most application code should ignore lanes and let the SDK default work.

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

Keep verification enabled in production. The SDK should grow explicit CA/SNI
options before it is used with private PKI that needs custom trust roots.

## Codecs

Use `FerricStore.Codec.Raw` for maximum simplicity and cross-language bytes.
Use `FerricStore.Codec.Term` only when all producers/workers are Elixir.

```elixir
queue = FerricStore.Queue.new(client, "email", codec: FerricStore.Codec.Raw)
```
