# Changelog

## 0.4.2

- Preserve already-decoded acknowledged results across fatal connection
  shutdown while failing unresolved work exactly once.
- Bound terminal acknowledgement draining and ignore stale request timeouts
  after response delivery has begun.
- Split connection, coordinator, and topology lifecycle work into focused,
  size-enforced runtime modules and stabilize asynchronous replacement tests.
- Repair the end-to-end KV benchmark and enforce acknowledged-response
  throughput floors in CI and release validation.

## 0.4.1

- Preserve acknowledged mutation results while topology changes gracefully
  drain or replace their native connections.
- Keep replacement sessions within configured capacity while pending requests
  finish, and retire overlapping sessions without terminating in-flight work.

## 0.4.0

- Require FerricStore 0.9.1 while retaining native wire protocol v1.
- Support per-state FIFO/parallel policies, deep patch and full replacement,
  generation compare-and-swap, and typed policy snapshots.
- Return a dedicated stale-policy-generation error and disable automatic retry
  for every mutation carrying `expected_generation`.
- Default `Workflow.install_policy/2` to full replacement.

## 0.3.0

- Require FerricStore 0.8.0 while retaining native wire protocol v1.
- Negotiate compact response codecs and response limits through `HELLO`.
- Enforce tokenized fetch completion, Flow fencing contracts, `max_active_ms`,
  canonical lineage fields, atomic single-slot MSET/MSETNX, and safe retries.
- Remove reserved Flow-key routing and tolerate future compact Flow extensions.
