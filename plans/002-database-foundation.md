# Plan 002: Establish transactional schema evolution and indexed cache metadata

> **Executor instructions**: Follow this plan step by step, run every gate, and
> stop on a STOP condition. Update the plan status in `plans/README.md`.
>
> **Drift check (run first)**:
> `git diff --stat ec6713a..HEAD -- src/db.rs src/cache.rs Cargo.toml Cargo.lock`

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: migration/perf/correctness
- **Planned at**: commit `ec6713a`, 2026-08-24

## Why this matters

The released v0.1 database can no longer rely on `CREATE TABLE IF NOT EXISTS`.
The current one-off migration does not atomically couple schema changes to its
version record. Queue, expiry, and cache queries also scan/sort tables that are
expected to grow, while every cache hit performs a second read and a
read-modify-write that can lose increments.

## Current state

- `src/db.rs:284-373` creates current entities before running migrations.
- `src/db.rs:376-400` checks only `nodes.route_prefix`; DDL and version record
  are not one explicit transaction.
- `src/db.rs:500-506` reads an entry and then writes `hit_count + 1`.
- `src/cache.rs:124-142` already read the same entry before calling touch.
- `src/cache.rs:218-230` loads every cache row to compute three aggregates.
- Queries needing indexes are at `src/image_tools.rs:245-248,974-977,1074-1095`
  and `src/auth.rs:367-374`.
- Repository DB tests live in `src/db.rs:570+`; cache concurrency tests live in
  `src/cache.rs:338+`.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| DB tests | `cargo test --locked db::tests cache::tests -- --nocapture` | all pass |
| Full tests | `cargo test --locked --all-targets` | all pass |
| Lint | `cargo clippy --locked --all-targets -- -D warnings` | exit 0 |
| Format | `cargo fmt --all -- --check` | exit 0 |

## Scope

**In scope**:
- `src/db.rs`
- `src/cache.rs`
- `Cargo.toml`, `Cargo.lock` only if a migration crate is justified; prefer the
  existing SeaORM transaction APIs and no new dependency.

**Out of scope**:
- Rebuilding tables to add foreign keys or CHECK constraints.
- Changing API response shapes.
- Moving SQLite or cache directories.
- Approximate/in-memory hit aggregation; retain exact durable counting here.

## Git workflow

- Branch: `advisor/002-database-foundation`
- Conventional commit: `perf: index control-plane queries` or equivalent.
- Do not push or open a PR.

## Steps

### Step 1: Turn migrations into an ordered transactional chain

Refactor `run_migrations` into ordered, idempotent versions. Query applied
versions, execute each unapplied migration and its version insert on the same
SeaORM transaction/connection, and commit only after all statements succeed.
Preserve compatibility with databases where `route_prefix` exists but version
1 is absent. Do not rebuild current tables.

**Verify**: add tests opening a v0-style file DB, running `connect` twice, and
asserting migration versions are present exactly once.

### Step 2: Add only access-pattern-backed indexes

In the next migration add indexes, with stable explicit names, for:

- `image_jobs(status, created_at)`
- `image_jobs(status, finished_at)`
- `image_sync_rules(enabled, next_run_at)`
- `admin_sessions(expires_at)`
- `oidc_login_states(expires_at)`
- `cache_entries(last_accessed_at)`

Use `IF NOT EXISTS`. Add a test using SQLite `EXPLAIN QUERY PLAN` or
`PRAGMA index_list` to prove the indexes exist after both fresh creation and
v0 upgrade. Do not add speculative indexes for small configuration tables.

**Verify**: `cargo test --locked db::tests -- --nocapture` -> migration/index
tests pass.

### Step 3: Make cache touch atomic and stats aggregate in SQL

Replace `touch_cache_entry` with one conditional SQL/SeaORM update:
`hit_count = hit_count + 1, last_accessed_at = now WHERE key = ?`. Add a DB
aggregate function returning count, non-negative byte sum, and non-negative hit
sum; make `CacheStore::stats` use it without loading models. Preserve the
existing public `CacheStats` shape.

Add a concurrent-touch test that proves no increments are lost, plus an
aggregate test with empty and populated tables.

**Verify**: `cargo test --locked db::tests cache::tests -- --nocapture` -> exact
counts match expected values.

## Test plan

- Fresh DB has all migrations and indexes.
- v0-style DB upgrades and a second startup is a no-op.
- Migration version is not recorded if a statement fails (inject or unit-test
  the transactional helper without corrupting production schema).
- Concurrent touch increments are not lost.
- Empty stats return zeros; populated stats match inserted rows.

## Done criteria

- [ ] Migration DDL and version recording share one transaction.
- [ ] Six named indexes exist on fresh and upgraded DBs.
- [ ] Cache hits use one atomic update after the initial lookup.
- [ ] Dashboard/cache stats do not load all cache models.
- [ ] Format, clippy, and all Rust tests pass.
- [ ] No public API field changes.

## STOP conditions

- SeaORM/SQLite cannot run the required DDL and version insert on the same
  transaction connection.
- Upgrade needs a destructive table rebuild.
- Existing production schema differs from the v0 fixture in a way that risks
  data loss; report the discovered schema before proceeding.

## Maintenance notes

Future foreign-key/CHECK migrations should build on this chain and include a
real v0.1 fixture plus `PRAGMA foreign_key_check` before table replacement.

