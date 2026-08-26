# UI Polish and Image Tools Reliability Plan

**Goal:** Polish every management form, clarify domain acceleration, fix image extraction lifecycle actions, and add an About page with version information.

**Architecture:** Keep Mantine form controls on a shared field grid with consistent helper-text slots. Route-aware defaults are derived from the selected Registry namespace, while image-tool lifecycle actions remain server-authorized and idempotent.

### Task 1: Form consistency and neutral defaults

**Files:** `frontend/src/pages/NodesPage.tsx`, `frontend/src/components/RegistryRoutesDialog.tsx`, `frontend/src/pages/DomainFoldPage.tsx`, `frontend/src/pages/ImageToolsPage.tsx`, `frontend/src/styles.css`.

- Remove promotional/example placeholders and sample destination values from every editable form.
- Align fields with descriptions by using consistent label/helper spacing and removing descriptions from only one side of a two-column row.
- Keep visible labels, validation messages, loading states, and press feedback.
- Verify all forms at desktop and mobile widths.

### Task 2: Registry namespace-aware defaults

**Files:** `frontend/src/pages/NodesPage.tsx`, `frontend/src/pages/ImageToolsPage.tsx`.

- When adding a node from a filtered namespace tab, preselect that namespace.
- Preserve the current route when editing.
- When selecting a source node for image extraction/copy, keep source credentials mutually exclusive and show the selected Registry namespace clearly.

### Task 3: Domain acceleration explanation

**Files:** `frontend/src/i18n.ts`, `frontend/src/pages/DomainFoldPage.tsx`.

- Explain that DomainFold accelerates GitHub Release assets, package files, and other configured public downloads, not Docker Registry pulls.
- Keep examples out of form values; examples belong in descriptive copy only.
- Provide equivalent Chinese and English copy.

### Task 4: Image extraction lifecycle

**Files:** `src/image_tools.rs`, `frontend/src/pages/ImageToolsPage.tsx`, `frontend/src/api.ts`, `tests/`.

- Do not expose the archive download action for extract jobs whose artifact is a directory.
- Allow deleting completed/failed/cancelled image jobs and their extracted content through an authenticated endpoint.
- Return a stable response when an artifact is not ready and add regression coverage for extraction download/delete behavior.

### Task 5: About page and navigation

**Files:** `frontend/src/pages/AboutPage.tsx`, `frontend/src/App.tsx`, `frontend/src/components/AppShell.tsx`, `frontend/src/api.ts`, `frontend/src/types.ts`, `frontend/src/i18n.ts`, `frontend/src/styles.css`.

- Add a responsive About page showing version, service status, project link, and security boundary.
- Add desktop and mobile navigation entries with localized labels.
- Read version from the public health endpoint rather than duplicating a hard-coded version.

### Task 6: Verification and release

- Run `cargo fmt --check`, `cargo clippy --locked --all-targets -- -D warnings`, `cargo test --locked --all-targets`.
- Run `pnpm --dir frontend lint` and `pnpm --dir frontend build`.
- Capture desktop/mobile screenshots for nodes, domain acceleration, image tools, settings, and About.
- Bump version, commit, push `main`, create a new tag and verify Docker/Release/LazyCat workflows.

