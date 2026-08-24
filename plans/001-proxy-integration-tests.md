# Plan 001: Add proxy-core integration characterization tests

> **Executor instructions**: Follow this plan step by step. Run every
> verification command before continuing. If a STOP condition occurs, stop and
> report instead of improvising. Update this plan's row in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat ec6713a..HEAD -- src/registry.rs src/scheduler.rs src/upstream.rs src/cache.rs src/server.rs tests`

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests
- **Planned at**: commit `ec6713a`, 2026-08-24

## Why this matters

The product's defining behavior is not covered end to end. Unit tests exercise
chunk arithmetic and small helpers, but not a request that enters the Registry
router, switches Range sources, verifies the final digest, admits the Blob, and
then serves a cache hit. These tests are the prerequisite for future scheduler,
transport, and module-boundary refactors.

## Current state

- `docs/SPEC.md:132-135` requires mock-Registry integration coverage for
  passthrough, failover, Range merge, and cache hits.
- `src/scheduler.rs:625-668` tests helpers only.
- `src/registry.rs:291-354` tests routing/ETag helpers, not full Blob transfer.
- `src/server.rs:206+` shows the existing Axum `ServiceExt::oneshot` test style.
- Cache objects are content-addressed and final digest verification is mandatory;
  tests must never weaken or bypass this check.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Focused tests | `cargo test --locked proxy_integration -- --nocapture` | all matching tests pass |
| Full Rust verification | `cargo test --locked --all-targets` | 40 existing plus new tests pass |
| Lint | `cargo clippy --locked --all-targets -- -D warnings` | exit 0 |
| Format | `cargo fmt --all -- --check` | exit 0 |

## Scope

**In scope**:
- `tests/proxy_integration.rs` (create), or a `#[cfg(test)]` support module if
  integration tests cannot access required public constructors.
- Minimal visibility-only changes in `src/lib.rs`, `src/registry.rs`,
  `src/scheduler.rs`, `src/upstream.rs`, `src/cache.rs`, or `src/server.rs`.

**Out of scope**:
- Production behavior changes other than a narrowly characterized capability-
  detection defect required to make the fallback/cache test pass. Any such fix
  must preserve SSRF, authentication, Range, and digest-verification behavior.
- Real network calls to Docker Hub or public mirrors.
- Relaxing SSRF checks, digest checks, response limits, or auth handling.

## Git workflow

- Branch: `advisor/001-proxy-integration-tests`
- Use conventional commits, e.g. `test: cover proxy range and cache flows`.
- Do not push or open a PR.

## Steps

### Step 1: Build a deterministic local Registry fixture

Create an Axum fixture bound to loopback and serving one known sha256 Blob.
Record HEAD/GET counts and requested Range headers. Allow each fixture node to
be configured as: valid 206 source, Range-unsupported 200 source, retryable 5xx
source, or corrupt/misaligned chunk source. Use ephemeral ports and shut every
listener down at test completion.

**Verify**: `cargo test --locked proxy_integration::fixture -- --nocapture` ->
fixture self-test passes without external network access.

### Step 2: Characterize Range merge and source failover

Drive the Donkey Registry router with a Blob GET whose digest matches fixture
bytes. Assert that valid chunks reconstruct exact bytes and digest, and that a
retryable failing node is replaced by a compatible healthy node. Also assert a
wrong `Content-Range` or corrupted chunk never reaches the cache.

**Verify**: `cargo test --locked proxy_integration::range -- --nocapture` -> all
Range/failover cases pass.

### Step 3: Characterize fallback and cache reuse

When Range support is absent, assert one full-source fetch succeeds. Repeat the
same Blob GET and assert the upstream GET count does not increase. Cover HEAD
and a client Range served from the completed cache object.

If reqwest reports a zero decoded body length for a successful HEAD despite a
valid positive `Content-Length` header, fix capability detection to parse the
wire header for HEAD metadata. Add a focused regression test for this exact
case. Do not reuse unverified probe bodies or relax length/digest validation.

**Verify**: `cargo test --locked proxy_integration::cache -- --nocapture` -> all
fallback/cache cases pass.

## Test plan

- Exact multi-chunk reconstruction.
- One failed source with successful replacement.
- Range-incompatible fallback to a full fetch.
- Bad Content-Range/corrupt bytes rejected and not admitted.
- Second GET is a zero-upstream cache hit.
- Cached HEAD and client Range semantics.

## Done criteria

- [ ] No test contacts a public host.
- [ ] At least five behavior cases above exist and pass.
- [ ] `cargo fmt --all -- --check` exits 0.
- [ ] `cargo clippy --locked --all-targets -- -D warnings` exits 0.
- [ ] `cargo test --locked --all-targets` exits 0.
- [ ] Only in-scope files and `plans/README.md` changed.

## STOP conditions

- A test requires disabling target validation or integrity verification.
- Required constructors cannot be exposed without changing production behavior.
- The existing implementation fails a characterization case for any reason
  other than the explicitly authorized HEAD `Content-Length` defect above:
  report the exact behavior before changing production code.

## Maintenance notes

Keep the fixture protocol-focused and deterministic. Future scheduler or
outbound-HTTP refactors must run this suite before module movement.
