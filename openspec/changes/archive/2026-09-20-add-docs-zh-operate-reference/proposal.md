## Why

After `add-docs-zh-components-reference` and `add-docs-zh-build-sql-guides`, 93 of 128 documentation pages have Simplified Chinese counterparts. The remaining 35 pages (~2.7k lines) are the operations/reference domain — `operate/` (11 + index), `control-plane/` (5), `reference/` (7), `develop/` (4), `deploy/`, `migration/`, `about/`, and the root `index.md`, `intro.md`, `start-here.md`, `contribute.md` — plus their sidebar categories still render English under `/zh-Hans/`. Translating them completes full-site zh-Hans coverage, the condition the original i18n proposal staged its remaining work behind.

## What Changes

- **Translate the operations/reference domain** (35 pages, mirrored paths): `operate/` all pages + `operate.md`, `control-plane/` (5), `reference/` (7), `develop/` (4), `deploy/` (1), `migration/` (1), `about/` (1), and root pages `index.md`, `intro.md`, `start-here.md`, `contribute.md`.
- **Localize the remaining sidebar category labels**: Operate, Control Plane, Reference, Develop, Migration, About in the docs-plugin i18n JSON.
- **Same contract as prior translation changes**: prose-only surface, identity front matter and `yaml` fences byte-identical, locale-aware links; English pages linking now-translated pages converted to absolute `/docs/...` routes when the gate flags them.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `documentation-i18n`: adds a coverage requirement — the operations, control-plane, and reference domain plus site-root pages SHALL be translated; completes full-site coverage intent alongside the two prior coverage requirements.

## Impact

- **`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/{operate,control-plane,reference,develop,deploy,migration,about}/**`** and 4 root pages — ~35 new translated files; sidebar JSON labels.
- **English pages** — possible link-route conversions flagged by `pnpm docs:check`.
- **`pnpm docs:check` and the localized Docusaurus build** must pass; no Rust, gate-script, or English-content changes beyond link conversions.

## Non-goals

- Versioned docs and blog posts — never translated per policy.
- The translation freshness report — separate follow-up proposal (`add-docs-zh-freshness-report`).
- Any reorganization, rewriting, or gate changes.

## Task Batches

1) operate + control-plane (17), 2) reference + develop (11), 3) root pages + deploy + migration + about (7), 4) reverse links + final validation.
