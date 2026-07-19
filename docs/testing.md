# Testing

## Unit tests

Run normal tests without FerricStore:

```bash
mix test --exclude integration
```

## Docker integration tests

Integration tests are explicit ExUnit integration tests and run against the
compatible FerricStore revision pinned by the SDK.

```bash
scripts/test_integration.sh
```

The script builds commit `11456cc0e5f099b72aac56ffe6acd8b6f3fd1624`, waits for
native startup, runs `mix test --only integration`, and removes the container.
Set `FERRICSTORE_TEST_IMAGE` to test a prebuilt compatible image instead.

Use `FERRICSTORE_TEST_PORT` when the local port is different:

```bash
FERRICSTORE_TEST_PORT=6389 scripts/test_integration.sh
```

## Full local gate

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix credo --strict
mix test --cover --exclude integration
mix run bench/sdk_hot_path_benchmark.exs \
  --iterations 1 --frames 1000 --packet-bytes 17 --keys 1000
mix hex.build
scripts/test_integration.sh
```

The coverage gate starts at 70% and is a ratchet: new code must not lower it.
Only generated `Inspect` protocol implementations are excluded from the report.
Raise the threshold as focused tests increase coverage.

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
