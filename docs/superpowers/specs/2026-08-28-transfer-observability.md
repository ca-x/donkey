# Spec: Transfer and Cache Observability

## Objective

Make Donkey's cache and transfer behavior measurable enough to explain cold
pulls, cache hits, upstream fallbacks, retries, and latency without changing
the correctness or latency contract of Registry requests. The feature is for
operators and maintainers; it must not become a second persistence path for
the proxy.

Success means an operator can distinguish a cache hit from an upstream Blob
download, calculate a real cache hit ratio, and compare cold versus warm
requests with percentile timing data while normal pulls continue to work if
metrics collection is unavailable or discarded.

## Scope

### In scope

- Process-local, bounded transfer counters and latency histograms.
- Cache lookup, hit, miss, admission, and cache-served byte accounting.
- Upstream-served byte and request accounting separated from cache-served
  bytes.
- Dashboard API fields and concise UI cards for the new aggregates.
- Deterministic integration coverage for cold/hot cache paths and metric
  accounting.
- An ignored local benchmark/diagnostic command that reports p50/p95/p99 for
  cold and warm Blob requests without public-network dependencies.

### Out of scope

- Any change to node selection formulas, BlobPlanner thresholds, chunk order,
  or retry policy.
- Per-request or per-digest labels in a persistent metrics store.
- SQLite writes on the Registry request hot path.
- An unbounded channel, queue, or metrics worker that can delay or reject a
  Registry request.
- New external telemetry dependencies or network exporters.
- Persisting process-local counters across restarts.

## Non-Functional Requirements

1. **Request-path safety**: metric recording is infallible and uses only
   bounded atomics/fixed buckets. No lock, allocation, filesystem I/O, SQLite
   I/O, or await is introduced solely for metrics.
2. **Failure isolation**: a metrics snapshot or serialization error cannot
   change a Registry response. Metrics are best-effort derived state.
3. **Bounded memory**: the metric object has a fixed number of counters and
   histogram buckets; it never stores URLs, digests, authorization values, or
   arbitrary labels.
4. **Monotonic accounting**: counters are non-negative and use saturating
   increments. A counter overflow must not panic or reduce a value.
5. **Source separation**: bytes sent from a verified cache object and bytes
   read from an upstream response are reported separately. A byte may be
   counted in the corresponding source exactly once.
6. **Backward-compatible API**: new JSON fields are additive. Existing
   dashboard consumers remain valid.
7. **No data migration**: metrics are process-local and require no schema or
   volume changes.

## Metric Contract

The control-plane dashboard exposes a `transfer_metrics` object with these
fields:

```text
blob_get_requests             external Blob GET requests evaluated for caching
blob_get_hits                 external Blob GET requests served from a valid object
blob_get_misses               external Blob GET requests that required an upstream fetch
blob_head_requests            external Blob HEAD requests evaluated for caching
blob_head_hits                external Blob HEAD requests served from a valid object
cache_hit_ratio               hits / (hits + misses), or null when denominator is zero
cache_bytes_served            bytes sent to clients from a verified cache object
cache_admissions_started      attempts to publish a completed object
cache_admissions_succeeded   successful object publication
cache_admissions_failed       failed publication attempts
cache_bytes_admitted          bytes in successful admissions
upstream_requests             requests sent to configured upstream nodes
upstream_bytes_fetched        bytes read from configured upstream nodes
retry_attempts                existing scheduler retry counter, exposed alongside these fields
```

`blob_get_requests`, `blob_get_hits`, and `blob_get_misses` are request-level decisions:
each external Blob GET/HEAD contributes at most one decision. Internal cache
rechecks after acquiring a lease are deliberately not counted, so the ratio is
not distorted by single-flight coordination.

All counters are exposed both as process-lifetime totals and as a fixed
five-minute rolling window. The API includes `started_at` and `window_seconds`
so consumers can tell which lifecycle they are viewing. A process restart resets
the lifetime totals and starts a new window.

Latency is represented as fixed millisecond buckets for four paths:

- `blob_cold_ttfb_ms`: request start to first response byte for a cache miss.
- `blob_cold_complete_ms`: request start to complete response for a cache miss.
- `blob_warm_ttfb_ms`: request start to first response byte for a cache hit.
- `blob_warm_complete_ms`: request start to complete response for a cache hit.

The API returns bucket counts and a sample count for both lifetime and window
views. Percentiles are calculated from those fixed buckets in the API/UI; no
individual timestamps or request identifiers are retained.

## Data Flow

```text
Registry request
  ├─ cache decision ─→ one atomic GET hit/miss counter per external Blob request
  ├─ cache hit ─────→ ServeFile ──→ cache-byte counter + warm histogram
  └─ cache miss ────→ upstream/scheduler
                     ├─ upstream request/fetched-byte counters
                     ├─ retry counter (existing)
                     └─ verified admission ──→ success/failure counters
```

The source of truth for cache contents remains the existing SQLite index plus
filesystem object store. Transfer metrics are a derived, process-local view;
they never decide whether an object is valid, evictable, or servable.

## Implementation Constraints

- Reuse the existing `TrafficMetrics`/scheduler atomic style where possible;
  do not add a second per-request database repository.
- Cache hit/miss counters are recorded at the external Blob request decision
  boundary. The implementation must keep the existing internal recheck after
  lease acquisition uncounted. Admission counters are recorded around the
  existing `admit` publication boundary.
- Source-byte accounting is attached to the response body wrappers that
  already observe body chunks. Metric updates must not alter backpressure or
  error propagation.
- Histogram bucket selection is deterministic and uses saturating atomic
  increments. The bucket layout is documented and tested.
- Existing response status, headers, digest verification, leases, retries,
  and cache eviction behavior remain unchanged.

## Testing Strategy

### Unit tests

- Concurrent counter increments do not lose updates.
- Hit ratio is null at zero denominator and exact for populated counters.
- Histogram bucket selection and percentile interpolation are deterministic.
- Saturating overflow does not wrap.

### Integration tests

- First full Blob request records one miss, upstream bytes, and one successful
  admission.
- Second request for the same Blob records a hit, cache-served bytes, and no
  additional upstream Blob transfer.
- A query-bearing Docker mirror request (`?ns=docker.io`) follows the same
  accounting path.
- A failed/cancelled admission increments failure accounting without changing
  the Registry response semantics.
- Dashboard serialization includes additive fields and preserves existing
  fields.

### Local diagnostic benchmark

Provide an ignored test or repository script that uses the existing loopback
fixture only. It runs a configurable number of cold and warm requests,
prints sample count, p50, p95, p99, hit/miss counts, source bytes, and retries,
and never contacts Docker Hub or another public mirror. Timing thresholds are
reported, not asserted, so CI remains stable.

## Commands

```bash
cargo fmt --all -- --check
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
pnpm --dir frontend install --frozen-lockfile --ignore-scripts
pnpm --dir frontend lint
pnpm --dir frontend build
# optional local diagnostic, explicitly ignored and loopback-only
cargo test --locked --test proxy_integration diagnostic -- --ignored --nocapture
```

## Rollback

Rollback is source-only: remove the metrics fields/UI and the process-local
metric object. No database migration, cache invalidation, or data conversion
is required. Existing cache objects and Registry behavior remain valid.

## Open Questions

- None for the first implementation. Algorithm changes remain a separate,
  benchmark-gated follow-up after this telemetry is available.
