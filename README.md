# FerricStore Elixir SDK

Elixir client for FerricStore over the native `ferric://` protocol.

## Install

```elixir
def deps do
  [
    {:ferricstore_sdk, "~> 0.1.0"}
  ]
end
```

## Connect

```elixir
{:ok, client} = FerricStore.start_link(url: "ferric://127.0.0.1:6388")
```

## KV

```elixir
:ok = FerricStore.set(client, "hello", "world")
"world" = FerricStore.get(client, "hello")
```

## Queue

```elixir
queue = FerricStore.Queue.new(client, "email", worker: "email-worker")
FerricStore.Queue.enqueue(queue, "email-1", payload: "send welcome")

FerricStore.Queue.run_once(queue, fn job ->
  # do work using job payload/attributes/values
  "sent"
end)
```

## Workflow

```elixir
workflow = FerricStore.Workflow.new(client, "order", initial_state: "reserved")
FerricStore.Workflow.start(workflow, "order-1", payload: "{}", attributes: %{tenant: "acme"})
```

## Benchmark

```bash
mix run bench/queue_benchmark.exs --flows 10000 --batch 100
```

## Integration tests

Run FerricStore locally or Docker:

```bash
docker run --rm -p 6388:6388 ghcr.io/ferricstore/ferricstore:0.5.2
FERRICSTORE_INTEGRATION=1 FERRICSTORE_URL=ferric://127.0.0.1:6388 mix test
```
