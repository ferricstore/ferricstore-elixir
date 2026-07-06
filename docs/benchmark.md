# FerricStore Elixir SDK benchmarks

Local comparison against the Python SDK on the same Docker server.

Environment:

- Client: local macOS
- Server: Docker `ghcr.io/ferricstore/ferricstore:0.7.2`
- Protocol: native `ferric://`
- Server URL: `ferric://127.0.0.1:6398`
- Protected mode disabled for local benchmark only

Server:

```bash
docker run --rm \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST=127.0.0.1 \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT=6398 \
  -p 6398:6388 \
  ghcr.io/ferricstore/ferricstore:0.7.2
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
