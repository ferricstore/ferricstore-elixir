# Development checks

Run these before publishing SDK changes:

```bash
mix format --check-formatted
mix compile --warnings-as-errors
mix test --cover --exclude integration
mix credo --strict
mix hex.build
```

Docker-backed integration suite:

```bash
scripts/test_integration.sh
```

## Architecture tests

Architecture rules live in `test/ferric_store/architecture/`.

The tests use `ArchTest.Collector.calls/2` so rules are based on compiled BEAM
call metadata without adding runtime code or request-path overhead.

Current rules:

- Protocol primitives own opcode metadata and must not call client/workflow or
  SDK-internal APIs.
- `FerricStore.Client` is a facade over `FerricStore.SDK.Native.Client` and must
  not own transport state.
- Native request validation/submission lives in `ClientRequests`; the
  coordinator owns state transitions and stays below the enforced source-size
  ceiling.
- `FerricStore.SDK.Native.Connection` is the only socket session and owns raw
  socket, frame stream, response identity, and outbound request encoding.
- The topology coordinator must delegate socket I/O and must not spawn a task
  per request.
- Admission, batch completion, connection-pool transitions, event subscription
  state, connection policy, route-key selection, and event delivery stay in
  focused modules rather than accumulating in the coordinator. Route epochs are
  opaque server change tokens and are not ordered numerically.
- User endpoint validation must remain isolated outside the topology
  coordinator.
- Codec modules must stay transport independent.
- Production modules must not contain debug `IO.puts`/`IO.inspect` calls.
- Production modules must not call `Process.sleep/1`.

## Performance check

After touching hot-path files, rerun the benchmark shapes in `docs/benchmark.md`.

The client-only parser and trusted grouped-KV preparation/response benchmark
does not need a running FerricStore server:

```bash
mix run bench/sdk_hot_path_benchmark.exs
```

Hot-path files include:

- `lib/ferric_store/client.ex`
- `lib/ferric_store/protocol.ex`
- `lib/ferric_store/sdk/kv.ex`
- `lib/ferric_store/sdk/native/client.ex`
- `lib/ferric_store/sdk/native/client_requests.ex`
- `lib/ferric_store/sdk/native/connection.ex`
- `lib/ferric_store/transport/frame_stream.ex`
- `lib/ferric_store/transport/socket.ex`
- `lib/ferric_store/flow.ex`
- `bench/queue_benchmark.exs`
- `bench/kv_benchmark.exs`

Workflow benchmark is noisy immediately after starting the Docker server. Wait a
few seconds after the first successful connection before treating one sample as
representative.
