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

## Review correction pass

The Task 3 review and controller browser QA identified four follow-up issues. This pass fixes them without changing backend contracts or adding dependencies:

- Node and Registry-route forms now expose `aria-busy`, disable their fieldsets, hide the close affordance, reject outside/Escape close, and disable Cancel while a save is pending. The route manager also lifts the editor-saving state to block manager close and create/edit transitions. Successful completion uses a dedicated current-editor completion path, so an originating save cannot be dismissed and reopened as another editor before its callback finishes.
- Route key and path prefix validation now uses the backend-equivalent `trim().toLowerCase()` normalization, excludes only the current route ID, and checks every sibling route before mutation. Invalid slash variants are rejected by the backend-matching identifier grammar; whitespace/case variants collide locally. Residual HTTP 409 saves use safe localized Chinese/English conflict copy while the backend remains authoritative.
- A new node selects only an enabled default/first route. With all routes disabled its initial `registry_route_id` is empty and required validation blocks submit; editing may retain its currently bound disabled route but cannot select a different disabled route.
- Mantine's dark primary shade changed from blue 5 (`#339af0`, 2.991:1 against white) to blue 8 (`#1971c2`, 5.021:1), exceeding WCAG AA 4.5:1 for the filled Add node button while retaining the filled primary hierarchy.

### Source-level behavior evidence

A dependency-free Node probe against the implemented normalization/default/contrast formulas reported:

```text
key whitespace/case duplicate: true
prefix whitespace/case duplicate: true
slash variant rejected: true
all-disabled default empty: true
white/blue-8 contrast: 5.021:1
```

Source review also confirmed both save modals bind `onClose`, `withCloseButton`, `closeOnClickOutside`, and `closeOnEscape` to pending state, and the route manager guards parent close/open/edit transitions. The controller will rerun browser/axe acceptance; no post-fix browser claim is made here.

### Fresh review-fix verification

- `pnpm --dir frontend lint` — passed.
- `pnpm --dir frontend build` — passed (`tsc -b`, Vite; 7,745 modules transformed).
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --locked --all-targets -- -D warnings` — passed.
- `cargo test --locked --all-targets` — passed: 67 unit tests and 9 integration tests, 0 failures.
- `cargo build --locked` — passed for Donkey v0.2.0.
- `git diff --check` — passed.
