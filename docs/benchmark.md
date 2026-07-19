# FerricStore Elixir SDK benchmarks

Local comparison against the Python SDK on the same Docker server.

## Client-only hot paths

Use the offline benchmark after changing frame buffering, response aggregation,
or connection dispatch. It measures fragmented frame append/decode, request
encoding, trusted-MGET preparation plus ordered reconstruction, and compact
MSET preparation across a 16-shard topology without needing a FerricStore
server. The fake coordinator reverses shard responses so the benchmark cannot
accidentally use the dense single-group shortcut:

```bash
mix run bench/sdk_hot_path_benchmark.exs \
  --iterations 5 \
  --frames 10000 \
  --body-bytes 32 \
  --packet-bytes 1337 \
  --keys 10000
```

CI also supplies maximum average latency and reduction budgets for frame
append/decode, bounded request encoding, MGET admission/reconstruction, and
MSET preparation. The benchmark exits non-zero when any budget is exceeded, so
the smoke workload is a performance regression gate rather than output-only
instrumentation. The MSET workload uses unrelated keys and therefore exercises
the canonical per-slot grouping policy, including the cost of many small slot
groups; it does not use the removed shard-level atomicity mode.

Environment:

- Client: local macOS
- Server: Docker image built from FerricStore commit
  `11456cc0e5f099b72aac56ffe6acd8b6f3fd1624`
- Protocol: native `ferric://`
- Server URL: `ferric://127.0.0.1:6398`
- Protected mode disabled for local benchmark only

Server:

```bash
FERRICSTORE_TEST_IMAGE=ferricstore-sdk-contract \
  scripts/build_integration_server.sh

docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6398 \
  -p 6398:6388 \
  ferricstore-sdk-contract
```

## KV throughput

Shape:

- `requests=100000`
- `clients=8`
- `batch=100`
- `value_bytes=32`

Elixir commands:

```bash
mix run bench/kv_benchmark.exs \
  --url ferric://127.0.0.1:6398 \
  --command set \
  --requests 100000 \
  --clients 8 \
  --batch 100 \
  --value-bytes 32

mix run bench/kv_benchmark.exs \
  --url ferric://127.0.0.1:6398 \
  --command get \
  --requests 100000 \
  --clients 8 \
  --batch 100 \
  --value-bytes 32
```

CI and release validation also run an acknowledged-response SET shape with no
pipeline batching. This catches delivery-acknowledgement regressions that the
offline codec benchmark cannot observe:

```bash
mix run bench/kv_benchmark.exs \
  --url ferric://127.0.0.1:6398 \
  --command set \
  --requests 1000 \
  --clients 4 \
  --batch 1 \
  --value-bytes 32 \
  --min-throughput 100.0
```

`--min-throughput` makes the command exit non-zero when measured requests per
second fall below the supplied floor.

Python comparison commands:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate

python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6398 \
  --command set \
  --requests 100000 \
  --threads 1 \
  --processes 1 \
  --clients 8 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 1 \
  --protocol-lanes 1 \
  --key-prefix py-elixir-compare-set \
  --key-count 100000 \
  --value-bytes 32 \
  --no-warmup \
  --pretty

python examples/protocol_kv_benchmark.py \
  --url ferric://127.0.0.1:6398 \
  --command get \
  --requests 100000 \
  --threads 1 \
  --processes 1 \
  --clients 8 \
  --pipeline 100 \
  --request-mode pipeline \
  --inflight-batches 1 \
  --protocol-lanes 1 \
  --key-prefix py-elixir-compare-get \
  --key-count 100000 \
  --value-bytes 32 \
  --no-warmup \
  --pretty
```

Results:

```text
Python SET:  44,935/s
Elixir SET:  46,019/s

Python GET:  866,849/s
Elixir GET:  1,315,789/s
```

Batch latency, pipeline/batch size `100`:

```text
Python SET: p50 17.31 ms, p95 19.45 ms, p99 20.77 ms
Elixir SET: p50 16.97 ms, p95 18.87 ms, p99 22.64 ms

Python GET: p50 0.69 ms, p95 0.84 ms, p99 1.24 ms
Elixir GET: p50 0.54 ms, p95 0.68 ms, p99 1.22 ms
```

## DBOS-style workflow throughput

Elixir command:

```bash
mix run bench/queue_benchmark.exs \
  --url ferric://127.0.0.1:6398 \
  --flows 100000 \
  --producers 16 \
  --workers 16 \
  --worker-connections 16 \
  --create-batch 500 \
  --create-mode many \
  --create-auto-buckets \
  --create-inflight-batches 2 \
  --claim-batch 500 \
  --claim-partition-batch 16 \
  --claim-drain-batches 2 \
  --complete-async-depth 1 \
  --server-shards 16
```

Python auto/no-partition throughput profile:

```bash
cd /Users/yoavgea/repos/ferricstore-python
. .venv/bin/activate

python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:6398 \
  --flows 100000 \
  --server-shards 16 \
  --profile throughput
```

Python explicit-partition comparison:

```bash
python examples/protocol_dbos_benchmark.py \
  --url ferric://127.0.0.1:6398 \
  --flows 100000 \
  --server-shards 16 \
  --profile throughput \
  --partition-mode explicit \
  --partitions 16
```

Results:

```text
Python auto/no-partition workflow:  ~72,112/s
Elixir workflow samples:            ~73,910/s, ~74,349/s, ~75,988/s
```

Notes:

- KV is in the same area as Python on SET and faster on this GET shape.
- Elixir workflow now uses the native multiplex client, auto-bucket `FLOW.CREATE_MANY`, compact claim responses, and batched terminal completion.
- For this local Docker shape, Elixir workflow throughput is in the same area as Python and slightly above the Python sample from this run.
- The Elixir workflow shape uses more client-side concurrency (`16` producer clients and `16` worker sockets). Python's sampled throughput profile uses a more efficient protocol worker with fewer worker connections, so future Elixir work should focus on reducing connection count without losing throughput.
- `Flow.claim_due/3` defaults to attributes for ease of use. The benchmark passes `include_attributes: false` to match Python's `claim_job_only` throughput profile.
