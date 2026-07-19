# Quickstart

## Install

```elixir
def deps do
  [
    {:ferricstore_sdk, "~> 0.4.0"}
  ]
end
```

```bash
mix deps.get
```

## Start FerricStore

This SDK requires FerricStore `~> 0.9.1`. The beta API contract changed at 0.9,
while native framing remains protocol v1.

From an SDK checkout, build the pinned server revision
`11456cc0e5f099b72aac56ffe6acd8b6f3fd1624` with the integration helper:

```bash
FERRICSTORE_TEST_IMAGE=ferricstore-sdk-contract \
  scripts/build_integration_server.sh

docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  ferricstore-sdk-contract
```

## Connect

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")
```

Use `ferric://` for plaintext local development. Use `ferrics://` for TLS.

## KV smoke test

```elixir
:ok = FerricStore.set(client, "hello", "world")
"world" = FerricStore.get(client, "hello")
```

## Queue item

```elixir
queue = FerricStore.Queue.new(client, "email", worker: "email-worker")

FerricStore.Queue.enqueue(queue, "email-1",
  payload: "welcome:user-1",
  attributes: %{tenant: "acme"}
)

FerricStore.Queue.run_once(queue, fn job ->
  # job is a map with id, lease_token, fencing_token, partition_key, attributes
  send_email(job["id"])
  "sent"
end)
```

If the handler returns `{:error, reason}`, `Queue.run_once/3` calls
`FLOW.FAIL`. Any other return value becomes the completion result.

## Workflow item

```elixir
workflow = FerricStore.Workflow.new(client, "order", initial_state: "created")

FerricStore.Workflow.start(workflow, "order-1",
  payload: "small routing payload",
  attributes: %{tenant: "acme"},
  values: %{order: :erlang.term_to_binary(%{total: 120})}
)

[job | _] = FerricStore.Workflow.claim(workflow, "created", limit: 1)

FerricStore.Workflow.transition(workflow, job["id"], "running", "charged",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  payload: "charged"
)
```

Claiming moves the durable state to `running`. Use `"running"` as the
`from_state` for a claimed job transition.

## Fetch state and history

```elixir
record = FerricStore.Flow.get(client, "order-1", payload: true)
history = FerricStore.Flow.history(client, "order-1")
```

Use history for debugging and audit, not replay.
