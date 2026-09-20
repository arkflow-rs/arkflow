## Context

Same translation contract as `add-docs-zh-components-reference` (see `openspec/changes/archive/2026-09-20-add-docs-zh-components-reference/design.md`): page-for-page mirroring under `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/`, prose-only translation surface, `yaml` fences byte-identical, locale-aware link rules, `pnpm docs:check` as the gate of record. This change extends it to the build/SQL/guides domain (37 pages, ~9.1k lines).

## Goals / Non-Goals

- **Goal**: complete, structurally faithful zh-Hans coverage for `build/`, `sql/`, `configuration/`, `how-to/`, `tutorials/`, `cases/`, `streaming-jobs.md`, `build-pipelines.md`; Recipes + SQL Reference sidebar labels localized.
- **Non-Goal**: operations/reference domain (follow-up proposal); freshness tooling; any English-content change beyond link-route conversion.

## Decisions

### D1: Batch by domain and volume
Six batches: build core (6) → build recipes (8) → sql small (5 + `sql.md`) → `sql/7-scalar_functions.md` (single ~4.8k-line page) → sql/5,8,9 + configuration (5) → how-to + tutorials + cases + 2 root pages (10). Gate (`pnpm docs:check`) after each batch.
*Alternative*: fewer, larger batches — rejected: the 4.8k-line function reference dominates; isolating it keeps failures attributable.

### D2: SQL function tables translated row-by-row with identifiers verbatim
In `sql/` reference tables, function names, signatures, return types, and SQL keywords stay verbatim; only `Description`/说明 cells are prose. SQL fences are not yaml — code stays verbatim; SQL line comments (`--`) may be translated following the shell-comment convention.

### D3: Recipe/case/how-to duplication translated consistently
`how-to/` largely mirrors `build/recipes/` content and `cases/` mirrors the numbered recipe cases. Translation of the recipe page happens first in its batch; the how-to/case counterpart reuses the same terminology per page pair so duplicated walkthroughs don't drift within zh-Hans.

### D4: Reverse link conversions applied mechanically
After each batch, run `pnpm docs:check`; for every flagged English page, convert the file-relative link into an absolute `/docs/<section>/<page>` route (strip numeric dir prefixes per Docusaurus routing; drop `.md`). English link text unchanged.

### D5: Sidebar labels for Recipes and SQL Reference
Localize `sidebar.tutorialSidebar.category.Recipes` → 实战配方 and `SQL Reference` → SQL 参考 in `current.json` (keys already exist).

## Risks / Trade-offs

- [Large single-page translation (sql/7) may exhaust an agent pass] → isolated batch; verify fences row-sample plus full fidelity script.
- [Terminology drift between recipes and their how-to/case mirrors] → translate page pairs with shared glossary (D3).
- [Link-rewrite churn in English pages] → mechanical, gate-checked (D4); no prose changes.

## Migration Plan

Additive-only plus mechanical English link-route conversions; rollback = remove added files and revert link commits.

## Open Questions

(none)
