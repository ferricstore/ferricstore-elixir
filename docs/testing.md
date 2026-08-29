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

The script runs the immutable FerricStore 0.11.14 release image, waits for native
startup, runs `mix test --only integration`, and removes the container. Set
`FERRICSTORE_TEST_IMAGE` to test another compatible image instead.

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
mix run bench/flow_query_client_benchmark.exs \
  --iterations 500 --records 100 --heap-samples 0 \
  --max-full-decode-reductions 24000 \
  --max-projected-decode-reductions 10000 \
  --max-count-decode-reductions 1000 \
  --max-raw-validation-reductions 300
mix hex.build
scripts/test_integration.sh
```

CI and release validation run both the offline Flow query allocation gate and
the acknowledged response benchmark from `docs/benchmark.md`. The latter runs
against the pinned server, and its throughput floor turns response-delivery
performance into a required gate.

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
