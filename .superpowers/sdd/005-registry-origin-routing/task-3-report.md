# Task 3 Report: Registry route UI and v0.2.0

## Status

Implemented the reviewed Task 3 scope without changing backend contracts, authentication, scheduling, CI, or release workflows.

## Delivered

- Added exact frontend `RegistryRoute`/summary/input types and CRUD API methods.
- Replaced frontend node kind/path-prefix fields with `registry_route_id` and route summaries; image-tool node labels now include the logical Registry.
- Nodes page now filters by Registry namespace, requires a namespace in create/edit forms, and displays logical Registry separately from mirror endpoint.
- Added a compact Nodes-page route manager with keyed first-paint-correct create/edit forms, enable/default controls, built-in deletion protection, named custom-route deletion confirmation, and an inline localized in-use conflict that preserves form state.
- Added bilingual validation/copy for route key, name, canonical Registry, path prefix, repository mode, and default-route semantics.
- Preserved existing Mantine focus/modal patterns, coarse-pointer 44 px controls, light/dark semantic tokens, press feedback, reduced motion/transparency, contrast, and forced-color behavior without new dependencies.
- Updated README/SPEC with Docker Hub and GHCR route examples, the usual Docker daemon `registry-mirrors` limitation, and the mandatory clean-database requirement for v0.2.0.
- Set Rust and frontend package versions to `0.2.0`; Cargo regenerated only Donkey's root lock entry.

## Fresh verification

- `pnpm --dir frontend lint` — passed.
- `pnpm --dir frontend build` — passed (`tsc -b`, Vite; 7,745 modules transformed).
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --locked --all-targets -- -D warnings` — passed.
- `cargo test --locked --all-targets` — passed: 67 unit tests and 9 integration tests, 0 failures.
- `cargo build --locked` — passed for Donkey v0.2.0.
- `git diff --check` — passed; frontend source contains no `NodeKind`, `node.kind`, `route_prefix`, or `routePrefix` references.

## Browser/manual evidence and limitations

Runtime browser acceptance was not run in this subtask because the controller explicitly reserved it for independent verification. English/Chinese, light/dark, keyboard/focus, 375 px, reduced preferences, route CRUD/in-use conflict, and node create/edit therefore remain browser-check items for the controller; this report makes no runtime claim for them. Source review confirmed both locales, responsive/reduced-preference CSS, stable modal keys, named confirmation, and preserved delete-error state.

## Concerns

No implementation blocker or known gate failure. v0.2.0 intentionally rejects an old database, so deployment must follow the documented backup and clean-database procedure.
