# FerricStore Elixir SDK benchmark

Initial local benchmark for the first Elixir SDK implementation.

Environment:

- Client: local macOS / Elixir SDK
- Server: Docker `ghcr.io/ferricstore/ferricstore:0.5.2`
- Protocol: native `ferric://`
- Client mode: one synchronous GenServer client, direct native Flow create/complete maps through pipeline
- Protected mode disabled for local Docker benchmark only

Command:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -p 6398:6388 \
  ghcr.io/ferricstore/ferricstore:0.5.2

mix run bench/queue_benchmark.exs \
  --url ferric://127.0.0.1:6398 \
  --flows 10000 \
  --batch 100
```

Result:

```text
flows=10000 elapsed_ms=4470 throughput=2237.14/s
```

Notes:

- This is not expected to match the Python SDK throughput yet.
- The current Elixir client is intentionally correctness-first: one socket, serialized calls, no multiplexed reader/writer, no compact response decoder.
- Next performance step is a protocol worker with request-id multiplexing and compact response decoding.
