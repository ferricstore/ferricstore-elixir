# Use Case Examples

These examples use the current Elixir SDK surface: `FerricStore.Flow`,
`FerricStore.Queue`, and `FerricStore.Workflow`. They intentionally keep payloads
small, use attributes for indexed metadata, and use named values/value refs for
larger state.

## Value-ref best practices

| Rule | Why |
| --- | --- |
| Keep `payload` small | Payload is often on the hot path. |
| Put large data in named values | States can hydrate only what they need. |
| Use stable `owner_flow_id` and `name` | Gives safe first-write semantics per flow. |
| Keep `override: false` by default | Prevents accidental replacement on retry. |
| Use `override: true` only intentionally | For regenerated drafts/reports. |

```elixir
meta = FerricStore.Flow.value_put(client, invoice_pdf,
  owner_flow_id: "order-1",
  name: "invoice_pdf",
  partition_key: "tenant-a:order-1",
  override: false
)

FerricStore.Flow.transition(client, job["id"],
  from_state: "running",
  to_state: "email_invoice",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  value_refs: %{invoice_pdf: meta["ref"]}
)
```

Fetch later:

```elixir
record = FerricStore.Flow.get(client, "order-1",
  partition_key: "tenant-a:order-1",
  values: ["invoice_pdf"],
  value_max_bytes: 512 * 1024
)
```

## Saga: order payment, inventory, shipment

Use explicit states for visible durable boundaries and compensation.

```elixir
workflow = FerricStore.Workflow.new(client, "order_saga",
  initial_state: "reserve_inventory",
  codec: FerricStore.Codec.Term
)

FerricStore.Workflow.start(workflow, "order-123",
  partition_key: "tenant-a:order-123",
  payload: %{tenant: "tenant-a", order_id: "order-123"},
  values: %{order: %{total: 120}},
  attributes: %{tenant: "tenant-a", kind: "checkout"}
)
```

Reserve inventory handler:

```elixir
[job | _] = FerricStore.Workflow.claim(workflow, "reserve_inventory", limit: 1)

reservation_id = Inventory.reserve(job["id"])

FerricStore.Workflow.transition(workflow, job["id"], "running", "charge_payment",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  values: %{reservation: reservation_id}
)
```

Charge payment handler:

```elixir
[job | _] = FerricStore.Workflow.claim(workflow, "charge_payment", limit: 1)
charge_id = Payments.charge(job["id"])

FerricStore.Workflow.transition(workflow, job["id"], "running", "ship_order",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  values: %{charge: charge_id}
)
```

Best practices:

- Use partition keys for order-level ordering.
- Use external idempotency keys for every side effect.
- Store step outputs as named values.
- Use compensation states instead of hiding compensation in a catch block.

## IoT fanout: command many devices

Use FerricFlow for durable orchestration and MQTT/AWS IoT/custom gateways for
the device network.

```elixir
devices = ["device-1", "device-2", "device-3"]

items =
  Enum.map(devices, fn device_id ->
    %{
      id: "firmware:rollout-7:#{device_id}",
      partition_key: "tenant-a:#{device_id}",
      payload: "install firmware v7"
    }
  end)

FerricStore.Flow.create_many(client, items,
  type: "device_command",
  state: "send",
  attributes: %{tenant: "tenant-a", rollout: "7"},
  independent: true,
  return_ok_on_success: true
)
```

Each device command is its own durable flow with lease, retry, timeout, history,
and attributes. The device gateway worker claims `type: "device_command"` and
sends over the network layer.

## AI orchestration

Use states for model/tool boundaries and named values for large prompts,
retrieval results, and model outputs.

```elixir
workflow = FerricStore.Workflow.new(client, "agent_run",
  initial_state: "retrieve_context"
)

FerricStore.Workflow.start(workflow, "agent-1",
  payload: "small request metadata",
  values: %{prompt: prompt_bytes},
  attributes: %{tenant: "acme", model: "reasoning"}
)
```

State pattern:

```elixir
[job | _] = FerricStore.Workflow.claim(workflow, "retrieve_context", limit: 1)
context_ref = FerricStore.Flow.value_put(client, context_bytes,
  owner_flow_id: job["id"],
  name: "retrieval_context",
  partition_key: job["partition_key"],
  override: false
)

FerricStore.Workflow.transition(workflow, job["id"], "running", "call_model",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  value_refs: %{retrieval_context: context_ref["ref"]}
)
```

This keeps large model context out of hot Flow state while preserving durable
debuggability.

## Batch imports

Use `create_many/3` for bursts. Use `independent: true` when one duplicate or
bad item should not fail the whole client-side import result.

```elixir
items =
  rows
  |> Enum.map(fn row ->
    {"import:#{row.id}", Jason.encode!(row)}
  end)

FerricStore.Flow.create_many(client, items,
  type: "import_row",
  state: "queued",
  independent: true,
  return_ok_on_success: true
)
```
