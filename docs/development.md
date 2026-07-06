# Development checks

Run these before publishing SDK changes:

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix test
mix credo --strict
mix hex.build
```

Docker-backed integration suite:

```bash
scripts/test_integration.sh
```

## Architecture tests

Architecture rules live in `test/ferric_store/architecture_test.exs`.

The tests use `ArchTest.Collector.calls/2` so rules are based on compiled BEAM
call metadata without adding runtime code or request-path overhead.

Current rules:

- `FerricStore.Protocol` must not call client/workflow APIs.
- `FerricStore.Client` must not call high-level Flow/Queue/Workflow APIs.
- Codec modules must stay transport independent.
- Production modules must not contain debug `IO.puts`/`IO.inspect` calls.
- Production modules must not call `Process.sleep/1`.

## Performance check

After touching hot-path files, rerun the benchmark shapes in `docs/benchmark.md`.

Hot-path files include:

- `lib/ferric_store/client.ex`
- `lib/ferric_store/protocol.ex`
- `lib/ferric_store/flow.ex`
- `bench/queue_benchmark.exs`
- `bench/kv_benchmark.exs`

Workflow benchmark is noisy immediately after starting the Docker server. Wait a
few seconds after the first successful connection before treating one sample as
representative.
