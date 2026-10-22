# Network and resource scheduling hardening

- Baseline: `a43ec74`.
- Status: DONE (`v0.4.1`).
- Requested: fix the verified network/scheduling findings, validate, bump the
  patch version, commit, and push the branch and a new annotated version tag.
- Work in the existing clean `main` checkout; do not create another branch or
  worktree. Preserve Registry authorization, digest verification, exact Range
  validation, resumable offsets, and atomic cache admission.

## Scope and implementation order

1. Correct parallel-benefit estimates in `src/blob_planner.rs` using the
   executable concurrency budget, rather than every candidate's throughput.
   Add a regression for ten Range sources with only two available connections.
2. Harden `src/registry.rs` retry/relay state transitions: classify transient
   HTTP status separately from malformed ranges, honor bounded Retry-After,
   enforce a cross-response no-progress budget, and cancel on client closure.
   Add deterministic loopback regressions, including initial success followed
   by transient Range failure and valid headers followed by empty bodies.
3. Share node request capacity across cached and streaming transfers, capability
   probes, health probes, and node-based image tools. Bound process-wide network
   concurrency and probe concurrency, and release leases on cancellation.
4. Represent unknown-length successful capability responses explicitly and
   fall back to a full single-source download with EOF/digest verification.
   Stop waiting for unrelated capability probes once a usable source is known:
   small Blobs start immediately; large Blobs use a short bounded discovery
   grace period. Keep size agreement and digest checks.
5. Enforce end-to-end stream byte budgets, including in-flight chunks, ordered
   results, and the downstream queue, plus a shared cross-Blob byte limit.
   Keep the budget alive until bytes leave internal buffers for the HTTP body. Drive
   already-started Range reads independently of downstream stalls; dropping
   their parent must abort the reads and release node/byte capacity.
6. Do not update node capacity from samples dominated by downstream backpressure
   or disk waits. Preserve success/failure and byte accounting separately.
7. Add integration coverage for the above interactions, update user-facing
   operational documentation, and synchronize package versions and lockfile.
8. Run formatting, all Rust tests, Clippy, frontend lint/build, local controlled
   diagnostics, and a fresh-context final review. Fix material review findings.
9. Confirm the remote branch and next version tag are unchanged, commit the
   complete patch, create an annotated patch tag, push without force, and read
   back branch/tag targets and workflow state. A tag push is not evidence that
   release artifacts or container builds have finished.

## Ownership and interfaces

- Planner worker: only `src/blob_planner.rs`; edits and tests, no commits.
- Relay worker: only `src/registry.rs` and `tests/proxy_integration.rs` until
  handoff; retries, cancellation, sampling, and their regression fixtures.
- Main agent: shared capacity, scheduler capabilities, stream byte-budget
  integration after relay handoff, image tools, documentation, versioning,
  final verification, commit, and remote operations.
- Shared capacity must be owned by `NodeService` so all clones and separately
  constructed upstream services see the same leases. Acquire byte capacity
  before node leases; do not hold node leases while waiting for buffered-byte
  capacity. Tests must cover cancellation and exhausted capacity.

## Verification

- `cargo fmt --all -- --check`
- `cargo test --locked --offline --all-targets`
- `cargo clippy --locked --offline --all-targets -- -D warnings`
- `pnpm --dir frontend lint`
- `pnpm --dir frontend build`
- `cargo test --locked --offline --test proxy_integration diagnostic -- --ignored --nocapture`
- Read back `git status --short --branch`, remote branch and annotated tag
  targets, and the CI runs for the pushed commit.

## Decisions and evidence

- Ruling: execute the requested bug fixes without an intermediate approval
  checkpoint; the user explicitly requested implementation and publication.
- Ruling: retain the existing selectable scheduling algorithms and conservative
  unmeasured-source policy. A new bandwidth controller or speculative hedging
  algorithm is not required to repair the verified defects.
- Ruling: release stream byte reservations when delivering a frame to the HTTP
  body consumer, not when arbitrary external `Bytes` clones are dropped. This
  bounds proxy-owned buffers without deadlocking a consumer collecting a body
  larger than one stream budget. In-flight tasks reserve before node leases.
- Ruling: acquire parallel chunk reservations in output order, before spawning
  independently driven, abort-on-drop Range tasks. This avoids out-of-order
  completions filling the byte budget while the next required chunk waits for
  memory. Sequential relays reserve a fixed bounded read/queue allowance before
  opening a node request and retain it through their queued frames.
- Ruling: byte-capacity waits also use the client idle timeout. Otherwise a
  full byte budget can stall before the message queue fills, bypassing the
  per-send timeout. The saturated-budget regression failed before this fix.
- Ruling: early discovery keeps cancelled/unobserved sources as deferred
  failover candidates, separate from confirmed planner sources. Whole and
  resume requests try confirmed sources before deferred sources; scheduling
  priorities must not send the first request to an unprobed slow source.
  Existing partial-resume tests and the priority-inversion end-to-end test
  exposed both sides of this requirement and now pass.
- Ruling: unknown-length relays classify upstream body/digest outcomes
  separately from downstream idle and local I/O errors. Local errors do not
  penalize a healthy upstream; they also cannot update capacity samples.
- Resource-budget regressions first failed for the 65th cross-node request and
  for 900 successful bytes with no speed sample; the shared limiter and separate
  byte accounting make both pass. Cancellation and shared-memory tests pass.
- Ruling: use an ignored offline Cargo home with existing registry/git caches
  because another project's package-cache write lock blocked all local tests.
- Baseline verification: 167 library tests and 31 proxy integration tests passed;
  two integration diagnostics are normally ignored.
- Baseline reproductions: the two-slot Planner chooses an overestimated
  multi-source plan; unknown-length loopback GET returns 502 with the normal
  fallback timeout but succeeds with immediate streaming.

## Final validation

- Rust 1.94.0: formatting and Clippy (`--all-targets -- -D warnings`) pass;
  187 library tests and 43 proxy integration tests pass. Five existing transfer
  benchmarks and two diagnostics remain ignored in the normal all-target gate.
- Both ignored proxy diagnostics pass when run explicitly. They use controlled
  loopback fixtures and do not establish WAN throughput or production latency.
- Frontend: pinned pnpm 11.3.0 lint, production build, and high-severity audit
  pass on Node 24.22.1; no dependency versions changed.
- The package, frontend package, and Cargo lockfile agree on version 0.4.1.
- The built binary passes a loopback startup smoke: `/api/health` reports
  0.4.1, and the console HTML and built JavaScript asset both return 200.
- Independent scheduling/resource and relay reviews report no remaining
  Critical/Important findings after the deferred-source, local-error outcome,
  and deterministic budget-saturation regression fixes. The latter regression
  passes three consecutive explicit runs plus the final all-target gate.
- Publication uses the annotated `v0.4.1` tag. CI and artifact completion are
  checked separately after pushing; a pushed tag alone does not prove that
  release binaries or container images have finished building.
- Regression evidence includes priority-inverted slow discovery, on-demand
  resume through an unprobed source, 429/503 and bounded Retry-After, empty-body
  retry termination, cancellation, slow clients, byte-budget saturation, and
  collection of a body larger than its internal memory budget.
