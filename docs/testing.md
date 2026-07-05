# Testing

## Unit tests

Run normal tests without FerricStore:

```bash
mix test --exclude integration
```

## Docker integration tests

Integration tests are explicit ExUnit integration tests and should run against
the published FerricStore Docker image.

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6388 \
  -p 6388:6388 \
  ghcr.io/ferricstore/ferricstore:0.7.1

mix test --only integration
```

Use `FERRICSTORE_TEST_URL` only when the local port is different:

```bash
FERRICSTORE_TEST_URL=ferric://127.0.0.1:6389 mix test --only integration
```

## Full local gate

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix test --exclude integration
mix hex.build
mix test --only integration
```

## Testing application code

For pure unit tests, hide FerricStore behind a small behaviour in your
application and use a fake implementation.

```elixir
defmodule MyApp.FlowStore do
  @callback enqueue(binary(), keyword()) :: term()
end
```

For integration tests, use the real SDK and Docker. Prefer unique flow ids in
every test:

```elixir
id = "test-flow-#{System.system_time(:nanosecond)}"
```

FerricStore data is durable; reused ids can collide across repeated local runs.
