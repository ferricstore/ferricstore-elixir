# Changelog

## Unreleased

- Distinguish locally unsent, definitely rejected, and uncertain durable
  mutations across native TCP and HTTP; keep HTTP 408, response loss, and
  future native statuses outcome-unknown without replaying the write.
- Make closed-client and request-preparation failures explicitly safe before
  submission, preserve server-compatible stale-lease rejections, and document
  caller-owned closure execution and server-time lease semantics.
- Verify release tag/version identity and compare an existing Hex package's
  immutable checksum before treating a publish retry as complete.
- Reject whitespace-only and invalid-UTF-8 durable step names before lease
  validation, closure execution, or network I/O.

## 0.12.0 - 2026-08-31

- Add chainable `FerricStore.Flow.advance/3` and durable
  `FerricStore.Flow.step/3`, with workflow-configured convenience wrappers,
  stable cross-SDK journal names, codec-consistent replay, strict lease and
  fencing validation, and a deprecated low-level `step_continue` migration
  path.
- Fail closed on malformed journal references, missing committed values, and
  incomplete full-record responses so a durable closure cannot be rerun from
  ambiguous metadata.
- Expose `FerricStore.Flow.DurableMutationOutcomeUnknownError` when a
  `FLOW.STEP_CONTINUE` response is lost or invalid, without automatically
  retrying a mutation that may already have committed.
- Cover the APIs with focused command tests and live native TCP, TLS HTTP/1.1,
  and HTTP/2 integration tests while keeping the existing command surface
  unchanged.
- Exercise live lease takeover, stale-worker fencing, external-effect
  idempotency, committed replay, and signal-driven waiting handoff on every
  supported transport.

## 0.11.14 - 2026-08-23

- Run the complete HTTP-compatible integration surface against an authenticated
  TLS listener in CI and release validation while preserving native-only
  session coverage.
- Replace the advisory-affected Cowboy/Cowlib test stack with Bandit and require
  a clean Hex advisory audit in CI and release gates.
- Document the reproducible HTTPS integration runner, keep its temporary TLS
  directory owner-only, and delete the CA key before the container starts.
- Reject sandbox and fetch-or-compute coordination commands locally over HTTP
  because they require state retained by a native TCP connection.

## 0.11.13 - 2026-08-23

- Preserve absolute request deadlines across the HTTP coordinator boundary and
  make synchronous caller death and asynchronous cancellation release monitored
  admission leases without exceeding the configured concurrency bound.
- Reject malformed HTTP options, duplicate or unsafe request headers, ambiguous
  typed-map responses, malformed server error metadata, and oversized raw
  binary payloads before JSON/Base64 allocation without raising.
- Keep generic PING payload behavior aligned with native TCP, fail HTTP event
  waits locally, and add direct HTTP/1.1 keep-alive and cancellation coverage.
- Support long-lived blocking commands and mixed ordered pipelines through the
  HTTP gateway. Finite sequential waits extend the implicit SDK deadline,
  `BLOCK 0` removes it, and explicit request deadlines remain authoritative.
- Validate the preserved native protocol v1 and FerricStore `~> 0.11.4`
  compatibility floor against the immutable FerricStore 0.11.10 Quay image.

## 0.11.12 - 2026-08-22

- Make native integration fixture shutdown idempotent when an intentionally
  failed connection races its linked fake server during test cleanup.

## 0.11.11 - 2026-08-22

- Mark HTTP-only live tests as skipped when the configured integration endpoint
  is native TCP, while retaining their dedicated HTTP SDK tag for HTTP URLs.

## 0.11.10 - 2026-08-22

- Keep HTTP-only live tests out of the native TCP integration filter while
  retaining their dedicated HTTP SDK integration coverage.

## 0.11.9 - 2026-08-22

- Add stateless HTTP and HTTPS transports behind the existing command, Flow,
  and pipeline APIs while retaining native TCP as the default.
- Reuse bounded Finch HTTP/1.1 pools and multiplexed HTTP/2 connections with
  TLS authentication, retained redirect headers, admission limits, bounded
  envelopes, and whole-request deadlines.
- Encode typed Flow commands through transport-neutral native descriptors,
  reject connection-affine operations locally, and cover the exact 67-command
  Flow surface against FerricStore HTTP and OSS 0.11.9.
- Preserve the FerricStore `~> 0.11.4` native compatibility floor and native
  wire protocol v1.

## 0.11.7 - 2026-08-22

- Validate the unchanged native protocol v1 and FerricStore `~> 0.11.4`
  compatibility floor against FerricStore 0.11.8, including authenticated and
  live integration coverage.
- Keep the existing native TCP command, pipeline, topology, Pub/Sub, and Flow
  query behavior unchanged while FerricStore adds transport-neutral gateway
  support.

## 0.11.6 - 2026-08-19

- Validate the unchanged native protocol v1 and FerricStore `~> 0.11.4`
  compatibility floor against FerricStore 0.11.6.
- Move live integration from GHCR to the immutable FerricStore 0.11.6 image on
  Quay.io.
- Make the GOAWAY reconnection test tolerate the short, valid overlap between a
  dead connection and its replacement in the client pool.

## 0.11.5 - 2026-08-03

- Negotiate FerricStore 0.11.5's compact Stream producer capability and encode
  homogeneous auto-ID `XADD` pipelines with mode 34 when callers request the
  existing compact return contract. Default maps, pair returns, request
  contexts, legacy servers, and unsupported XADD grammar retain the generic
  pipeline path.
- Negotiate compact Pub/Sub mode 35 and encode homogeneous `PUBLISH channel
  message` pipelines with the same compact-return opt-in. Legacy servers,
  default return shapes, request contexts, and unsupported command grammar
  retain the generic native pipeline path.
- Retain FerricStore `~> 0.11.4` compatibility and native wire protocol v1.

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
