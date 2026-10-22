# Skopeo-inspired image tools

Source: issue #3 and https://github.com/podman-container-tools/skopeo.

## Objective and scope

Reuse Donkey's Registry clients, source credentials and mirror routes to inspect
images before pulling and to avoid unnecessary local storage during Registry copy.
No Docker daemon, external skopeo process, new dependency or schema migration.

| Capability | Responsibility | Depends on |
| --- | --- | --- |
| metadata-resolution | Resolve manifest/config separately from layer download | Existing source adapters |
| image-inspection | Display digest, platform, compressed size, layers and config | metadata-resolution |
| incremental-copy | Check destination layers before downloading; transfer one missing layer at a time | metadata-resolution |

Implementation order: metadata-resolution → image-inspection, incremental-copy.

## Acceptance criteria

- An authenticated read-only inspect endpoint fetches only manifest and config,
  honors direct-source credentials or an explicitly selected mirror, validates
  platform/source inputs, and creates no job, layout, layer file or archive.
- Image tools includes an inspection form and localized results with digest,
  actual config platform, compressed size, layer descriptors and raw config.
- Copy checks target Blob existence before source layer access. Existing layers
  cause no download/upload; missing layers reuse local shared Blobs when present,
  otherwise use one temporary file that is deleted after transfer/failure.
- Copy publishes the manifest only after all required Blobs are present. Cancellation
  remains effective between transfers. Unchanged scheduled copies skip before
  downloading layers. Export/file extraction keeps the existing layout workflow.
- No new persistent full-image copy layout. Missing layer temporary storage is
  bounded by the per-layer export limit; no measured CPU/RAM savings are claimed.

## Implementation and verification

Rust code: `src/image_tools.rs`, existing adapters in `src/image_tools/`.
React: existing Mantine components in `frontend/src/pages/ImageToolsPage.tsx`,
API/types and Chinese/English resources. Follow existing snake_case Rust and
camelCase TypeScript; use `ApiResult` and the existing notification conventions.

Commands:

```sh
cargo fmt --check
cargo test --lib
cargo test --test proxy_integration
corepack pnpm --dir frontend build
corepack pnpm --dir frontend lint
```

Tests use existing httpmock fixtures to assert zero layer requests for inspect
and destination hits, verify missing-layer upload ordering and temporary cleanup,
and protect source-route validation. Verify the rendered form/results at desktop
and narrow widths if a browser is available. Always preserve credential separation,
digest validation and bounded streaming. Never modify public `/v2` write semantics.

## Verification results

- `cargo test --locked --all-targets`: 198 passed, 0 failed, 7 existing ignored tests.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- Frontend TypeScript/Vite build, ESLint and dependency audit: passed.
  Release preparation updated Vite to 8.3.5 and pinned patched brace-expansion
  2.1.7/5.0.12; source-map-js is resolved to a patched version in the lockfile.
- `cargo clippy --locked --all-targets -- -D warnings -A deprecated`: passed.
  Without `-A deprecated`, Rust 1.99 reports the pre-existing `fetch_update`
  use in `src/transfer_metrics.rs:500`; this file was not changed.
- Browser checks with controlled API fixtures: desktop 1280px and mobile 375px,
  English/Chinese, read-only member, inspect form/results, no document overflow
  or browser errors. Temporary screenshots are outside the repository.
- Live smoke test: `ghcr.io/home-assistant/home-assistant:stable` returned 200,
  linux/amd64, 22 layers, 652,725,077 compressed bytes; jobs, shared Blob and partial
  directories remained empty. Docker Hub returned 502 at the upstream connection;
  an independent curl to registry-1.docker.io also timed out. Destination copy,
  authentication separation and cleanup are covered by httpmock tests.

Status: DONE (implemented for v0.4.0; published state is tracked in GitHub Releases).
