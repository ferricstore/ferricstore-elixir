# Quickstart

## Install

```elixir
def deps do
  [
    {:ferricstore_sdk, "~> 0.12.2"}
  ]
end
```

```bash
mix deps.get
```

## Start FerricStore

This SDK requires FerricStore `~> 0.11.4`. The beta API contract changed at 0.11,
while native framing remains protocol v1.

From an SDK checkout, run the immutable server image validated by this release:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  quay.io/ferricstore/ferricstore:0.11.14@sha256:f7d29befefa15bce4b3755bf786cf7620c814f13bbd336c0d9955581b323b60e
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

{job, charge} =
  FerricStore.Workflow.step(workflow, job,
    name: "charge-customer:v1",
    run: fn ->
      Stripe.charge(
        amount: 150,
        idempotency_key: "#{job["id"]}:charge-customer:v1"
      )
    end,
    to_state: "charged"
  )

job = FerricStore.Workflow.advance(workflow, job, to_state: "receipt")
```

`step/3` journals the closure result with a stable name and returns the refreshed
claim plus the stored result. A replay returns that result without rerunning the
closure. The provider idempotency key is still required for the failure window
between a successful external call and FerricStore committing the result.

Both `step/3` and `advance/3` keep the workflow claimed and update its logical
`run_state`. To wait for a timer, signal, or approval without occupying a worker,
persist the waiting business state with `transition/5` and the refreshed claim's
lease and fencing tokens. See the workflow guide for the complete handoff.

## Fetch state and history

```elixir
record = FerricStore.Flow.get(client, "order-1", payload: true)
history = FerricStore.Flow.history(client, "order-1")
```

Use history for debugging and audit, not replay.
