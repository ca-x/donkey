# Plan 005: Model registry origins and mirror endpoint groups

## Status

- **Decision**: approved as a clean, intentionally incompatible refactor
- **Priority**: P1
- **Effort**: L; more than eight files because schema, routing, image tools,
  API, frontend, tests, and docs must change together
- **Risk**: MED
- **Depends on**: plans 002 and 003
- **Planned at**: `ec6713a` plus reviewed optimization branch `88e5692`, 2026-08-24

## Goal

Represent the relationship between a logical image registry and one or more
mirror endpoints explicitly. For example:

| Logical Registry | Donkey path namespace | Mirror endpoint |
|---|---|---|
| Docker Hub (`docker.io`) | default / no prefix | `https://docker.1ms.run` |
| GHCR (`ghcr.io`) | `ghcr` | `https://ghcr.1ms.run` |

An incoming request resolves exactly one logical Registry route first, then
selects and ranks nodes belonging to that route. It must never fail over from
one logical Registry to another.

## Why the current model is insufficient

- `src/registry.rs:198-234` derives known path prefixes from currently enabled
  nodes. Disabling the last node can therefore change path parsing and make the
  same URL mean a different repository.
- `src/nodes.rs:88-101` groups nodes by nullable text `route_prefix`, but the
  database does not record which canonical Registry the group represents.
- `src/image_tools.rs` allows selecting any Registry node for a source image;
  it cannot prove that a Docker Hub node belongs to `docker.io` or a GHCR node
  belongs to `ghcr.io`.
- The existing `domain_mappings` table is for ordinary file URLs and must not
  be reused for OCI Registry routing or authentication.

## Chosen design

Add a normalized `registry_routes` source-of-truth table. A route represents a
logical Registry namespace; `nodes` remain the replaceable/health-ranked mirror
endpoints belonging to that route.

```text
incoming /v2/<prefix>/<repo>/...
              |
              v
      registry_routes (one route)
      - canonical registry
      - path namespace
      - repository normalization
              |
              v
        nodes (many endpoints)
      docker.1ms.run, mirror A, mirror B
              |
              v
      scheduler / auth / cache
```

The route is authoritative configuration. Node health and cache contents are
derived state. This avoids making request semantics depend on which node happens
to be enabled at the moment.

## Schema

Create `registry_routes` through the transactional migration chain:

| Column | Type | Rules |
|---|---|---|
| `id` | UUID primary key | immutable |
| `key` | text | normalized lowercase, unique, `[a-z0-9][a-z0-9_-]{0,31}` |
| `name` | text | 1–80 characters |
| `canonical_registry` | text | normalized lowercase host[:port], no scheme/path |
| `path_prefix` | nullable text | normalized like key; null only for default route |
| `repository_mode` | text | `docker_hub_library` or `passthrough` |
| `is_default` | boolean | at most one enabled/default route |
| `enabled` | boolean | disabled routes return explicit unavailable errors |
| timestamps | UTC | created/updated |

Indexes and invariants:

- unique `key`;
- unique non-null `path_prefix` via partial unique index;
- at most one `is_default = true` via partial unique index;
- index `nodes(registry_route_id, enabled, priority)`;
- application validation in this additive release; foreign keys/CHECK table
  rebuild remains part of the previously deferred constraint migration.

Replace `nodes.route_prefix` with required `nodes.registry_route_id`. Define a
real database foreign key to `registry_routes.id` with `RESTRICT` deletion and
a unique `(registry_route_id, url)` index. Remove both `route_prefix` and the
origin-like `kind` field from the node ORM/API/frontend: Registry identity now
lives only in `registry_routes`, avoiding contradictory `kind=ghcr` plus a
Docker route. There is no dual-read or dual-write layer.

## Built-in routes and fresh schema

Seed deterministically by stable key, not random identity:

1. `dockerhub`: canonical `docker.io`, default route, no path prefix,
   `docker_hub_library` normalization.
2. `ghcr`: canonical `ghcr.io`, path prefix `ghcr`, `passthrough` normalization.

No historical database migration or backfill is implemented. The software has
not entered use, so this release defines a new schema baseline. Startup against
the old node schema must fail clearly with an instruction to remove/recreate the
database; it must not guess a route or silently rewrite data.

## Request routing rules

1. Parse the repository and operation marker as today.
2. Resolve the first repository segment against enabled `registry_routes`, not
   enabled nodes. If it matches a prefix, select that route and strip exactly
   one path segment.
3. Otherwise select the enabled default route.
4. Apply `library/` only when the selected route uses
   `docker_hub_library` and the repository has one segment.
5. Select only enabled nodes with the resolved `registry_route_id`.
6. If the route is disabled or has no eligible nodes, return an explicit 503;
   never fall back to another route.
7. Preserve query strings and existing Registry validators/auth behavior.

Examples:

- `donkey.example/library/nginx:latest` -> Docker Hub route ->
  `docker.1ms.run/v2/library/nginx/...`
- `donkey.example/ghcr/org/image:tag` -> GHCR route ->
  `ghcr.1ms.run/v2/org/image/...`

Docker daemon `registry-mirrors` normally applies to Docker Hub only. GHCR and
other registries still require rewritten image references, containerd per-host
configuration, or a future Donkey host-routing feature. This plan does not
claim transparent interception of arbitrary Registry hostnames.

## Authentication and Image Tools

- Node-specific Basic/Bearer/custom-header credentials stay on `nodes`; secrets
  are not copied into routes.
- Bearer token cache remains keyed by the selected node and credential
  generation; a route never shares a token between mirror endpoints.
- When Image Tools uses `source_node_id`, normalize the source image Registry
  (`docker.io` aliases included) and require it to equal the node route's
  `canonical_registry` before any network request.
- Every custom route requires a canonical Registry before it can be saved.
- Destination copy credentials remain bound to the destination Registry and
  are unaffected.

## Cache decision

Keep verified sha256 Blob storage globally content-addressed. The same digest
means the same bytes, so cross-route deduplication is safe after final digest
verification. Preserve authorization scoping in the cache key. Do not add
`route_id` to verified Blob object identity merely to mirror the routing model;
that would duplicate immutable bytes without improving correctness.

Non-digest or future manifest caches must include `registry_route_id`,
repository, reference, platform, and relevant authorization identity.

## API and UI

Add admin CRUD for `/api/registry-routes` and a read-only route summary for
members. Deletion is rejected while nodes reference the route; built-in routes
may be disabled but not deleted.

Node input requires `registry_route_id`. Node responses expose a route summary;
the legacy `route_prefix` request and response field is removed.

On the Nodes page:

- replace free-text route prefix in the normal path with a required
  “Registry namespace” selector;
- show logical Registry and mirror endpoint separately;
- place custom route CRUD in a compact management dialog on the Nodes page,
  not a new top-level navigation item;
- require canonical Registry and path namespace when creating custom routes.

All new copy is bilingual and uses existing light/dark/accessibility behavior.

## Files in scope

- `src/db.rs` — new schema baseline, entities, foreign keys, seeds, indexes.
- `src/registry.rs` — authoritative route resolution.
- `src/nodes.rs` — route-bound node validation and selection.
- `src/image_tools.rs` — source Registry/node compatibility validation.
- `src/api.rs` — Registry-route endpoints and DTOs.
- `frontend/src/api.ts`, `types.ts`, `pages/NodesPage.tsx`, `i18n.ts`.
- `tests/proxy_integration.rs` and focused unit/integration tests.
- `docs/SPEC.md`, `README.md`, `.env.example` only where behavior/configuration
  must be documented.

No additional service, database engine, runtime, third-party API, token, or
account is required.

## Explicitly not building

- Host/SNI-based multi-listener routing such as
  `docker.donkey.example` versus `ghcr.donkey.example`.
- Automatic discovery that a third-party mirror truly mirrors a claimed
  canonical Registry; administrator configuration is authoritative.
- Reuse of DomainFold mappings.
- Registry push through the pull-through listener.
- Multi-instance control-plane coordination or distributed route consensus.
- Compatibility with the old SQLite schema or legacy node API.

## Verification

Backend/database tests:

- fresh DB seeds exactly one Docker default and one GHCR route;
- startup detects the old node schema and fails with a recreate-database error;
- uniqueness/default invariants reject ambiguous routes;
- disabling the last node does not change path parsing;
- disabled/missing route returns 503 and never uses the default group;
- Docker Hub alone receives `library/` completion;
- `docker.1ms.run` and `ghcr.1ms.run` rows sharing credentials/priority never
  cross-fail over;
- Image Tools rejects a selected node whose canonical Registry differs from the
  source image before network access;
- custom route input without canonical Registry is rejected at the API boundary;
- route deletion is rejected by both application validation and the database
  foreign key while nodes reference it.

Proxy integration tests:

- Docker and GHCR fixtures expose different bytes for the same tag and each
  request reaches only its route;
- query, HEAD, Range, digest verification, cache hit, and bearer/custom-header
  behavior remain intact;
- identical verified Blob digest can reuse content-addressed storage without
  allowing a differently scoped authorization key to reuse it.

Frontend/manual acceptance:

- create Docker and GHCR routes/nodes, then verify examples shown above;
- edit a node and confirm its route is initialized on first modal paint;
- route deletion confirmation and “in use” error preserve state;
- Chinese/English, light/dark, keyboard, 375px, reduced motion and axe WCAG AA.

Commands:

```text
pnpm --dir frontend lint
pnpm --dir frontend build
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo build --locked
```

## Release and rollback

Ship as `v0.2.0` because backend schema, routing semantics, API, and node form
change together. Before starting the new binary, delete the unused v0.1 SQLite
database/data directory. Verify fresh initialization and the two example pulls,
then merge all feature commits into `main`, push `main`, create/push the
`v0.2.0` tag, and verify release-binary and multi-architecture container Actions.

Rollback means stopping v0.2.0, removing its unused database, and starting the
previous binary with a fresh data directory. There is intentionally no database
rollback compatibility.

## Fragile assumption

This plan assumes path namespaces are acceptable for non-Docker-Hub registries,
for example `donkey.example/ghcr/org/image`. If the actual requirement is
transparent pulls using separate Donkey hostnames with no path prefix, Host/SNI
routing and certificate/listener configuration become part of the design and
this plan must be revised before implementation.
