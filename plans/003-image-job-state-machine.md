# Plan 003: Make image-job transitions conditional and recoverable

> **Executor instructions**: Execute only after plan 002. Run each verification
> gate and stop rather than improvising on schema drift. Update the status row.
>
> **Drift check (run first)**:
> `git diff --stat ec6713a..HEAD -- src/image_tools.rs src/db.rs frontend/src/pages/ImageToolsPage.tsx frontend/src/types.ts`

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/002-database-foundation.md`
- **Category**: correctness/architecture
- **Planned at**: commit `ec6713a`, 2026-08-24

## Why this matters

Job status, stage, timestamps, cancellation, retry, and lease fields currently
change through unrelated string assignments. A worker performs a non-atomic
read-then-update claim, restart resets every running job regardless of lease,
and the API permits retry/cancel requests without checking the source state.
These allow duplicate or contradictory work as automation grows.

## Current state

- `src/image_tools.rs:191-202` resets all running jobs to pending on startup.
- `src/image_tools.rs:242-260` selects pending then updates by primary key only.
- `src/image_tools.rs:1316-1328` cancels any status.
- `src/image_tools.rs:1331-1348` retries any status.
- Persistent strings are part of the v0 API/frontend contract; preserve their
  serialized spelling.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Focused tests | `cargo test --locked image_tools::tests -- --nocapture` | all pass |
| Full backend | `cargo test --locked --all-targets` | all pass |
| Frontend contract | `pnpm --dir frontend build` | exit 0 |
| Lint/format | `cargo fmt --all -- --check && cargo clippy --locked --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**:
- `src/image_tools.rs`
- `src/db.rs` only for repository helpers/index migration following plan 002.
- `frontend/src/pages/ImageToolsPage.tsx`, `frontend/src/types.ts` only if the
  compiler requires exhaustive status handling; do not change response fields.

**Out of scope**:
- Parallel job execution or multi-instance support claims.
- New queue infrastructure, message broker, or distributed consensus.
- Splitting `image_tools.rs` in the same change.
- New persistent fields unless the existing lease fields prove insufficient;
  if they do, STOP and propose a separate migration.

## Git workflow

- Branch: `advisor/003-image-job-state-machine`
- Conventional commit: `fix: guard image job transitions`.
- Do not push or open a PR.

## Steps

### Step 1: Define and test allowed transitions

Introduce private typed `JobKind` and `JobStatus` parsing/serialization helpers
or an equivalent centralized transition layer. Keep v0 strings unchanged.
Define: pending -> running/cancelled; running -> completed/skipped/failed/
cancelled; failed/cancelled/skipped -> pending on retry; completed and active
jobs cannot retry; terminal jobs cannot cancel. Central helpers must update the
related timestamps, error, cancel flag, and lease consistently.

**Verify**: table-driven tests cover every allowed and rejected transition.

### Step 2: Claim with compare-and-set semantics

After selecting the oldest pending ID, update it with a condition on both ID
and `status = pending`. Continue only when exactly one row changed, then refetch
the claimed model. A racing worker that changes zero rows must retry the next
tick without executing the stale model. Set running/stage/start/lease/update
fields in that one conditional update.

**Verify**: a test with two concurrent claim attempts proves only one obtains
the same job.

### Step 3: Recover only abandoned work

At service startup, requeue only running jobs whose lease is null or expired;
do not reset an unexpired running job. Refresh `lease_until` during persisted
progress/stage updates so ordinary long jobs remain live. Document that v0.1
still uses one worker per process and does not provide cross-process fencing.

**Verify**: startup recovery tests cover expired, null, and future leases.

### Step 4: Guard API cancel and retry

Use conditional updates based on allowed source statuses and return a conflict
or bad-request response when no legal transition exists. Preserve not-found
semantics by distinguishing an absent ID from an illegal existing state.

**Verify**: router/service tests cover cancel pending/running, reject cancel
terminal, retry failed/cancelled, and reject retry pending/running/completed.

## Test plan

- Full status transition matrix.
- Two concurrent claims, one winner.
- Startup recovery respects lease expiry.
- Cancel/retry legal and illegal states.
- Existing scheduled copy/export/extract tests remain green.

## Done criteria

- [ ] No unconditional reset of all running jobs remains.
- [ ] Claim update filters on `status = pending` and checks affected rows.
- [ ] Cancel/retry enforce explicit source states.
- [ ] Persistent/API strings remain compatible.
- [ ] Rust format, clippy, tests, and frontend build pass.

## STOP conditions

- Correctness requires claiming safe multi-process support without an ownership
  field/fencing token; report and propose a separate schema plan.
- Existing tests depend on retrying completed or active jobs.
- A public API response-field change becomes necessary.

## Maintenance notes

True multi-instance workers require lease owner + attempt/fencing conditions on
every progress/final update. Do not advertise that capability after this plan.

