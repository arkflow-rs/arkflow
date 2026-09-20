## Why

`add-docs-zh-components-reference` completed the zh-Hans components reference, leaving 72 untranslated pages (per the mirror-diff of `docs/docs/` against `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/`). The largest remaining cluster for stream-processing builders is the build/SQL domain: `build/` (14 pages incl. all recipes), `sql/` (9 pages + `sql.md` index), `configuration/` (2), `how-to/` (5), `tutorials/` (1), `cases/` (3), and the root pages `streaming-jobs.md` and `build-pipelines.md` — 37 pages (~9.1k lines) that a Chinese-speaking user needs to actually build and tune pipelines. `sql/7-scalar_functions.md` alone is ~4.8k lines of function reference tables, the single most consulted page in the section.

## What Changes

- **Translate the build/SQL/guides domain into the zh-Hans tree** (37 pages, mirrored paths): `build/` (backpressure, delivery-semantics, distributed-jobs, exactly-once, metadata, wal, recipes index + 7 recipes), `sql/` (all 9) + `sql.md`, `configuration/` (2), `how-to/` (5), `tutorials/` (1), `cases/` (3), `streaming-jobs.md`, `build-pipelines.md`.
- **Translation surface unchanged**: titles/descriptions, headings, prose, table prose cells; identity front matter, `components:` lists byte-equal; `yaml` fences byte-identical (SQL/code fences keep code verbatim).
- **Sidebar labels** for the Recipes and SQL Reference categories localized in the docs-plugin i18n JSON.
- **Linking rules respected in both directions** — after translating `build/recipes/`, any English page that file-relative-links into translated pages must switch to absolute `/docs/...` routes (`pnpm docs:check` flags these).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `documentation-i18n`: adds a coverage requirement — the builder-facing guides and the SQL reference SHALL be translated, extending component-reference coverage; structural-fidelity, English-canonical, and never-translated rules unchanged.

## Impact

- **`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/{build,sql,configuration,how-to,tutorials,cases}/**`** and two root pages — ~37 new translated files; sidebar JSON entries for Recipes/SQL Reference.
- **`docs/docs/build/recipes/**`** and other English pages — file-relative links into now-translated pages converted to absolute `/docs/...` routes (gate-enforced).
- **`pnpm docs:check` and the localized Docusaurus build** must pass; no Rust or gate-script changes.

## Non-goals

- Translating `operate/`, `control-plane/`, `reference/`, `develop/`, `deploy/`, `migration/`, `about/`, and the remaining root pages (`index`, `intro`, `start-here`, `contribute`) — follow-up proposal.
- The translation freshness report — separate follow-up proposal.
- Versioned docs and blog posts — never translated per policy.
- Rewriting or reorganizing any English page beyond link-route conversions.

## Task Batches

 sized by volume: 1) build/ core (6), 2) build/recipes (8), 3) sql small pages (6), 4) sql/7-scalar_functions (1, ~4.8k lines), 5) sql/5,8,9 + configuration (5), 6) how-to + tutorials + cases + root pages (10).
