# Changelog

## Unreleased

- Add bounded `fields:` projections to every FQL-backed Flow collection helper
  while retaining complete records when the option is omitted.
- Build compact Flow query records once and validate typed record lists in one
  bounded traversal without copying raw convenience results.
- Size-gate Flow query decoder heap preallocation, leaving tiny and non-query
  responses on the default heap.
- Add CI and release reduction budgets for complete, projected, count, and raw
  Flow query client paths.

## 0.11.4 - 2026-07-28

- Document the complete durable-schedule recurrence response, including
  creation time, interval period, cron expression, timezone, and overlap retry
  configuration.
- Require FerricStore `~> 0.11.4` while retaining native wire protocol v1.

## 0.6.2 - 2026-07-27

- Add typed specialized-plan capabilities and complete query-index service,
  field, lifecycle, validation, retirement, and statistics status structs.
- Enforce server-aligned query parameter, diagnostic, cursor, quality, usage,
  and selected-index response bounds before exposing results.
- Preserve binary query text while using allocation-light ASCII keyword checks
  for explain routing and the existing bounded native request path.
- Pin live integration and the reviewed OSS fallback revision to FerricStore
  0.11.3 while retaining `~> 0.11.0` compatibility and native wire v1.

## 0.6.1 - 2026-07-26

- Validate the unchanged compact FQL1 query/result contract against
  FerricStore 0.11.2's fused index execution and corrected compact
  `EXPLAIN ANALYZE` response path.
- Pin live integration and the reviewed OSS fallback revision to FerricStore
  0.11.2 while retaining `~> 0.11.0` compatibility and native wire v1.

## 0.6.0 - 2026-07-26

- Require FerricStore 0.11.0 while retaining native wire protocol v1 and the
  existing FQL1 query/result contracts.
- Add typed `%QueryIndex{}` and `%QueryIndexFormat{}` management responses with
  bounded covering fields and opaque per-generation codec identities.
- Reject missing, duplicate, oversized, invalid UTF-8, and malformed nullable
  index metadata, with focused and live OSS catalog coverage.

## 0.5.1 - 2026-07-24

- Require FerricStore 0.10.3 for result projections and the negotiated compact
  FQL1 result codec while retaining native wire protocol v1.
- Add `FerricStore.Flow.QueryProjection` for bounded, source-aware sparse
  run/event selectors and validate the decoder against the shared server corpus.
- Ignore well-formed unknown future response codecs while retaining global
  opcode uniqueness and strict validation for supported codecs.
- Negotiate the named FQL1 result codec without enabling the broader compact
  Flow response surface.

## 0.5.0

- Require FerricStore 0.10.0 and negotiate the complete OSS FQL1 query,
  explain, index-status, result-shape, and diagnostic contracts during HELLO.
- Add typed `query/4`, `explain/4`, `explain_analyze/4`, and
  `query_indexes/3` APIs with bounded inputs, opaque cursors, actionable
  diagnostics, and exact unsigned 64-bit index generations.
- Compile collection convenience functions to partition-scoped FQL and remove
  the superseded collection opcodes from the native command surface.
- Cover pagination, count, explain/analyze, index status, eventual projection,
  and scoped query ACL behavior in unit and live integration tests.
- Pin live integration to the immutable FerricStore 0.10.2 release and exact
  OSS core commit while retaining `~> 0.10.0` compatibility.
- Reject incompatible index-status contracts during HELLO, validate FQL text
  identifiers and explain fingerprints, and preserve Flow metadata
  normalization in collection query builders.
- Keep `list`, `search`, `terminals`, `failures`, lineage, and `stuck`
  conveniences on the unified query opcode, and reject unbounded collection
  shapes before transport.
- Default updated-time collection conveniences to newest-first order so they
  use native descending indexes; pass `rev: false` for explicit ascending order.
- Reject malformed UTF-8 query response text and quality labels over 64 bytes
  before returning server metadata.

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
