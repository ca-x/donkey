# Transfer Observability Implementation Plan

> **For agentic workers:** Implement task-by-task with a fresh verification gate after each task.

**Goal:** Add bounded cache/upstream transfer telemetry and a loopback-only diagnostic benchmark without changing Registry behavior.

**Architecture:** A shared process-local `TransferMetrics` object uses saturating atomics and fixed five-minute buckets. Cache and Registry paths record source-specific events; the admin API snapshots metrics and the dashboard renders additive fields. No metrics event performs database or filesystem I/O.

**Tech Stack:** Rust atomics/HTTP body streams, Axum JSON API, React/TypeScript, existing loopback proxy fixtures.

**Spec:** `docs/superpowers/specs/2026-08-28-transfer-observability.md`

## Global Constraints

- Do not change node selection, BlobPlanner, chunking, retry, digest, lease, or eviction behavior.
- Do not add SQLite writes, awaits, locks, allocations, labels, or external network calls to metrics recording.
- New API fields are additive; existing consumers and Registry responses remain unchanged.
- Benchmark is ignored, loopback-only, and reports timing rather than asserting thresholds.

### Task 1: Metrics core

Implement fixed counters, five-minute buckets, ratio snapshots, histogram buckets, and unit tests for saturation/concurrency/percentiles.

### Task 2: Cache and Registry instrumentation

Record one external GET decision, separate HEAD counters, admissions, cache bytes, upstream fetched bytes, and cold/warm latency without changing response flow.

### Task 3: API and console

Expose additive `transfer_metrics` JSON and render hit ratio/source bytes/latency summaries in the dashboard and cache page in both locales.

### Task 4: Diagnostic benchmark

Add an ignored local fixture benchmark for cold/warm requests and print p50/p95/p99 plus source counters and retries.

### Task 5: Full verification

Run format, all Rust tests, Clippy, frontend lint/build, and the ignored benchmark; review the diff for hot-path side effects.
