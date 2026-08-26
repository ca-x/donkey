# Resumable Blob Download Implementation Plan

**Goal:** Resume interrupted large Blob downloads across configured Registry nodes without serving corrupt cache objects.

**Architecture:** Keep complete objects in the existing content-addressed cache. For large Range-capable Blobs, persist a digest-keyed partial file and resume from its byte length; each retry validates `206 Content-Range`, and completion performs a full SHA-256 check before atomic cache admission. Partial files are bounded by TTL and removed on mismatch.

**Tech Stack:** Rust, Axum, reqwest, Tokio, existing CacheStore/Scheduler.

## Global Constraints

- Small Blobs and non-Range upstreams retain the current scheduler path.
- Partial data is never exposed as a complete cache object.
- Digest verification is mandatory before cache admission.
- All timeout values come from Config with defaults.

### Task 1: Range resume primitives

**Files:** Modify `src/scheduler.rs`; test `tests/proxy_integration.rs`.

- Add digest-keyed partial path helpers and a resume download method.
- Send `Range: bytes=<offset>-` when a partial file exists.
- Require `206` and matching `Content-Range`; restart from zero on invalid metadata.
- Retry the remainder on the next node after transport/5xx/429/408 failures.
- Verify final size and SHA-256 before `CacheStore::admit`.

### Task 2: Partial lifecycle and configuration

**Files:** Modify `src/config.rs`, `src/cache.rs`, `src/api.rs`, `frontend/src/types.ts`, `frontend/src/pages/SettingsPage.tsx`, `frontend/src/i18n.ts`.

- Add `DONKEY_PARTIAL_TTL` (default `1h`) and expose it read-only in Settings.
- Remove stale partial files at startup/cleanup according to TTL.

### Task 3: Regression coverage

**Files:** Modify `tests/proxy_integration.rs`.

- Add a fixture that drops a connection after a prefix and assert the next node receives a suffix Range request.
- Assert corrupt/mismatched ranges never enter cache.
- Run unit, integration, lint, and frontend builds.

