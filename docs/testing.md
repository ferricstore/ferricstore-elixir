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
scripts/test_integration.sh
```

The script starts `ghcr.io/ferricstore/ferricstore:0.7.2`, waits for native
startup, runs `mix test --only integration`, and removes the container.

Use `FERRICSTORE_TEST_PORT` when the local port is different:

```bash
FERRICSTORE_TEST_PORT=6389 scripts/test_integration.sh
```

## Full local gate

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix test --exclude integration
mix hex.build
scripts/test_integration.sh
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
