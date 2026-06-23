# Data, Attributes, and Value Refs

FerricFlow separates hot workflow state from larger values.

## Payload

Payload is the current-state payload. Keep it small and only hydrate it when a
handler needs it.

```elixir
FerricStore.Flow.create(client, "flow-1",
  type: "email",
  payload: "small routing payload"
)

record = FerricStore.Flow.get(client, "flow-1", payload: true)
```

Use `payload_max_bytes` to cap hydration:

```elixir
FerricStore.Flow.get(client, "flow-1",
  payload: true,
  payload_max_bytes: 64 * 1024
)
```

## Attributes

Attributes are small indexed metadata for filtering and debugging.

```elixir
FerricStore.Flow.create(client, "flow-1",
  type: "order",
  attributes: %{tenant: "acme", region: "us"}
)

FerricStore.Flow.list(client,
  type: "order",
  state: "queued",
  attributes: %{tenant: "acme"},
  count: 100
)
```

Do not put large payloads in attributes. Use value refs for large data.

## Named values and value refs

Use named values when different states need different pieces of data.

```elixir
FerricStore.Flow.create(client, "order-1",
  type: "order",
  payload: "small routing payload",
  values: %{
    order: :erlang.term_to_binary(%{total: 120}),
    receipt_template: "template bytes"
  }
)
```

Store reusable values explicitly:

```elixir
meta = FerricStore.Flow.value_put(client, "large report bytes",
  owner_flow_id: "order-1",
  name: "fraud_report",
  override: false
)

ref = meta["ref"]
["large report bytes"] = FerricStore.Flow.value_mget(client, [ref])
```

Use stable `owner_flow_id` and `name` for idempotent first-write semantics.
Keep `override: false` unless replacing the value is intentional.

## Codecs

The SDK ships two codecs.

`FerricStore.Codec.Raw` is the default:

```elixir
queue = FerricStore.Queue.new(client, "email", codec: FerricStore.Codec.Raw)
```

It sends binaries directly and converts non-binaries with `to_string/1`.

`FerricStore.Codec.Term` is useful for Elixir-only systems:

```elixir
workflow = FerricStore.Workflow.new(client, "order",
  initial_state: "created",
  codec: FerricStore.Codec.Term
)

FerricStore.Workflow.start(workflow, "order-1", payload: %{total: 120})
```

For cross-language workflows, define an explicit JSON, MessagePack, Protobuf, or
Avro codec in your application.

```elixir
defmodule MyJsonCodec do
  @behaviour FerricStore.Codec

  def encode(value), do: Jason.encode_to_iodata!(value) |> IO.iodata_to_binary()
  def decode(value), do: Jason.decode!(value)
end
```
