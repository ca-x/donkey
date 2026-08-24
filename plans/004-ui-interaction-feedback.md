# Plan 004: Restore immediate, accessible UI feedback

> **Executor instructions**: Follow this plan in order. Use the existing Mantine
> component system and bilingual copy. Run all gates and update the plan status.
>
> **Drift check (run first)**:
> `git diff --stat ec6713a..HEAD -- frontend/src frontend/package.json frontend/pnpm-lock.yaml`

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: none
- **Category**: bug/accessibility/UX
- **Planned at**: commit `ec6713a`, 2026-08-24

## Why this matters

The UI is visually coherent, but several interactions violate directness and
agency: edit forms initialize after their entrance animation, destructive
resources disappear on one click, some async rows lack pending feedback, and
user motion/transparency/contrast preferences are only partially honored. The
goal is calm, predictable behavior—not additional decoration.

## Current state

- `frontend/src/pages/NodesPage.tsx:168-215` and
  `DomainFoldPage.tsx:43-47` initialize forms on `onEnterTransitionEnd`.
- Cache deletion already has the confirmation pattern to reuse at
  `CachePage.tsx:81-97`.
- Node, mapping, sync-rule, and credential deletion are immediate.
- `ImageToolsPage.tsx:145-173` omits row-specific pending states for rule and
  credential mutations; jobs poll every two seconds even when idle.
- `styles.css:702-731` handles some reduced motion but not Mantine modal/progress
  behavior, reduced transparency, high contrast, or small touch targets.
- `AppShell.tsx:94` exposes a focusable main region but never focuses it on
  navigation.
- Reuse Mantine, Tabler icons, Sonner/Mantine notifications, existing CSS
  variables, bilingual `i18n.ts`, and the restrained non-advertising copy.

## Commands you will need

| Purpose | Command | Expected on success |
|---|---|---|
| Lint | `pnpm --dir frontend lint` | exit 0 |
| Type/build | `pnpm --dir frontend build` | exit 0 |
| Backend contract | `cargo test --locked --all-targets` | all pass |

## Suggested executor toolkit

- Read `/home/czyt/.codex/skills/apple-design/SKILL.md` before implementation.
- Use the existing Mantine hooks, including reduced-motion media queries; do
  not add an animation dependency for this plan.

## Scope

**In scope**:
- `frontend/src/App.tsx`
- `frontend/src/components/AppShell.tsx`
- `frontend/src/pages/NodesPage.tsx`
- `frontend/src/pages/DomainFoldPage.tsx`
- `frontend/src/pages/ImageToolsPage.tsx`
- `frontend/src/pages/CachePage.tsx` only to extract/reuse confirmation behavior.
- `frontend/src/styles.css`, `frontend/src/theme.ts`, `frontend/src/i18n.ts`
- One small reusable component/hook under `frontend/src/components/` if it
  removes duplicated confirmation or motion logic.

**Out of scope**:
- New gradients, particle backgrounds, decorative loops, haptics, or sounds.
- Redesigning information architecture or changing backend APIs.
- Trigger-positioned popover physics or replacing all modals with sheets.
- Adding a new frontend test framework in this plan.

## Git workflow

- Branch: `advisor/004-ui-interaction-feedback`
- Conventional commit: `fix: make control-plane interactions predictable`.
- Do not push or open a PR.

## Steps

### Step 1: Initialize forms before presenting them

Remove business-state updates from modal animation callbacks. When a node or
mapping dialog opens or its target changes, synchronously/React-effectfully set
the correct values before interactive content is presented. Reset dirty/errors
at the same point. Opening one item after another must never flash stale values.

**Verify**: lint/build pass; manually open create, item A, item B in sequence and
confirm the first painted fields match the target.

### Step 2: Add consistent destructive-action confirmation

Extract a small confirmation dialog or reuse one local pattern for node,
mapping, sync-rule, and Registry-credential deletion. Show the resource name
and a specific consequence. Default focus must remain on Cancel; destructive
button uses red and shows loading. Add concise Chinese and English strings.
Do not add confirmation to reversible/non-destructive actions.

**Verify**: lint/build pass; each delete requires confirmation, Escape/cancel
preserves the resource, and double activation cannot issue duplicate requests.

### Step 3: Make async feedback local and non-repeatable

Bind create/run/delete/cancel/retry loading to the affected button/row using
mutation variables. Disable only the relevant action while pending. Add logout
failure feedback. Change image-job polling to two seconds only while a pending
or running job exists; use a slower interval or no polling while idle, and
invalidate immediately after create/retry/run.

**Verify**: lint/build pass and React Query does not issue duplicate mutations
from repeated clicks.

### Step 4: Complete preference and touch adaptations

At coarse-pointer/mobile breakpoints, make interactive icon/tab/input targets
at least 44px and input font size at least 1rem without enlarging glyphs.
Reduced motion must disable modal scale/pop and animated progress while keeping
short color/opacity feedback. Add `prefers-reduced-transparency`,
`prefers-contrast: more`, and `forced-colors` fallbacks for modal blur and key
boundaries. Keep current restrained materials; do not add glass to every card.

**Verify**: lint/build pass; inspect 375px light/dark and emulated reduced
motion/contrast. No horizontal overflow at 320px.

### Step 5: Restore route wayfinding and press feedback

On pathname changes only, update `document.title` from the localized page name
and focus `#main-content` with `preventScroll`; never steal focus on query
refreshes. Apply a consistent pointer-down state to enabled Mantine buttons and
action icons, with transform disabled but color feedback retained under reduced
motion.

**Verify**: keyboard navigation announces/focuses the new page once; theme and
language controls retain focus-visible styling.

## Test plan

No test dependency is added here. Manual verification must cover:

- Create/edit A/edit B form first paint.
- Cancel and confirm for all four destructive resource types.
- Row-specific pending states and mutation errors.
- 320px/375px, keyboard-only, light/dark.
- reduced-motion, reduced-transparency, contrast-more/forced-colors.
- Route focus changes only on navigation.

## Done criteria

- [ ] Modal form values never depend on animation completion.
- [ ] Four destructive resource types require named confirmation.
- [ ] Pending mutations disable only their triggering controls.
- [ ] Active-job polling is adaptive.
- [ ] Mobile targets are at least 44px and inputs avoid iOS focus zoom.
- [ ] Reduced motion/transparency/contrast behavior is explicit.
- [ ] Route changes update title and main focus.
- [ ] Frontend lint/build and full Rust tests pass.

## STOP conditions

- A required behavior needs a backend API change.
- A shared confirmation component makes resource-specific consequences vague.
- Mantine transition overrides cannot respect reduced motion without changing
  every call site; report the smallest central alternative.

## Maintenance notes

Add Playwright/component coverage for these behaviors before the next broad UI
redesign. Trigger-anchored popovers and mobile sheets are a separate spatial
motion project, not a quick CSS follow-up.
