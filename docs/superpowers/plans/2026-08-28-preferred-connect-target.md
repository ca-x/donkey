# Preferred Connect IP or DNS Name Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow each upstream node to override its connection target with either a literal public IP or a DNS hostname such as `saas.sin.fan`, while preserving the upstream URL host for TLS SNI and HTTP routing.

**Architecture:** Reuse the existing persisted `connect_ip` column and API property for backwards compatibility, broaden validation to accept a hostname, resolve the override at transport construction, and pin all resolved public addresses into the reqwest client. The UI label and helper copy will describe both accepted forms.

**Tech Stack:** Rust/Axum/Reqwest/SeaORM, Tokio DNS lookup, React/TypeScript/Mantine, Rust unit tests.

**Spec:** User request: support preferred IP or preferred domain for an upstream node.

## Global Constraints

- Reject private, reserved, loopback, link-local, and unspecified resolved addresses unless private-upstream mode is explicitly enabled.
- Preserve the original upstream URL host as TLS SNI and request authority.
- Keep existing literal-IP configurations working without a database migration.
- Do not make the `cf_preferred` marker silently change scheduling behavior.

---

### Task 1: Validate and resolve preferred connect targets

**Files:**
- Modify: `src/security.rs`
- Modify: `src/nodes.rs`
- Modify: `src/api.rs`
- Modify: `src/upstream.rs`
- Test: Rust unit tests in the touched modules

- [ ] Add a shared async resolver that accepts an IP literal or hostname, resolves all addresses, and applies the existing public-address policy.
- [ ] Replace API/node CRUD IP-only checks with hostname-aware validation.
- [ ] Apply the resolver when building node transports and bearer-token transports; keep URL host unchanged.
- [ ] Add tests for literal IP, hostname resolution, invalid host, and private-address rejection.

### Task 2: Update management UI and translations

**Files:**
- Modify: `frontend/src/pages/NodesPage.tsx`
- Modify: `frontend/src/i18n.ts`

- [ ] Rename the field copy to “Preferred connect IP / domain”.
- [ ] Explain that a domain is resolved to connection IPs while TLS SNI remains the upstream URL host.
- [ ] Keep the existing `connect_ip` JSON property and form behavior.

### Task 3: Verify behavior and documentation

**Files:**
- Modify: `README.md`
- Test: Rust and frontend verification commands

- [ ] Document an example using `https://box.w0x7ce.eu/` with `saas.sin.fan` as the preferred DNS target.
- [ ] Run formatting, targeted tests, all Rust tests, frontend lint/build, and inspect the final diff.
