## Context

`add-docs-i18n-zh` established the zh-Hans localization layer: locale registration (`docs/docusaurus.config.ts:72`), chrome JSON translations, first-mile page translations, and zh-tree quality gates in `docs/scripts/docs-check.mjs` (front-matter parity, link/anchor resolution inside the zh tree, byte-identical YAML fences). The components reference (39 pages under `docs/docs/components/` + `docs/docs/components.md`) is untranslated. The translation contract is already documented (`docs/DOCUMENTATION.md:98-103`) and enforced; this change applies it at scale for the first time.

## Goals / Non-Goals

- **Goal**: every page under `docs/docs/components/` (plus the `components.md` section index) has a structurally faithful Simplified Chinese counterpart; the localized sidebar and localized pages read naturally for a Chinese-speaking plugin user.
- **Non-Goal**: translating any other section (follow-up proposals); changing gates, config, or English content; freshness tooling (separate proposal).

## Decisions

### D1: Page-for-page mirroring, batched by component kind
Translate to `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/<same-relative-path>.md`, batched `1-buffers` → `2-processors` → `0-inputs` → `3-outputs` → `4-temporary` → `5-codecs` → `components.md`, so `pnpm docs:check` can be run per batch and failures localize to one kind.
*Alternative*: translate in one pass — rejected: a 40-page single pass makes gate failures hard to attribute.

### D2: Terminology follows the shipped first-mile translations
Reuse the glossary already in the tree: 输入(Input)/输出(Output)/处理器(Processor)/缓冲(Buffer)/编解码器(Codec)/流(Stream)/流水线(Pipeline); first occurrence carries the English in parentheses (`输入(Input)`). Code identifiers — config field names, type names, component type-strings (`kafka`, `sql`, …), `__meta_` columns — stay untranslated; component proper nouns (Kafka, MQTT, Debezium) stay as-is. Config-table `Field/Type/Required/Default` columns stay verbatim; only `Description` cells are translated.
*Alternative*: a fresh glossary — rejected: drift from the already-shipped 16 pages would read inconsistently.

### D3: Headings get stable IDs the same way English pages do
Docusaurus generates heading slugs from the heading text, so translated headings produce different anchor slugs than English. Anchor links inside the zh tree are checked by `docs:check`; where an English page links to its own anchors (`#configuration` …), the translated page rewrites intra-page anchors to the translated heading's slug. Cross-page file-relative links are kept only when the target is also translated (inside `components/` everything is); links out of the section use locale-prefixed absolute routes (`/zh-Hans/docs/...`).

### D4: Sidebar category labels localized via `current.json`
Add `sidebar.tutorialSidebar.category.*` entries for the components categories (Inputs, Buffers, Processors, Outputs, Temporary, Codecs and any nested labels) to `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current.json`, matching the existing pattern used for Get Started/Build.

### D5: `components.md` section index translated with the same rules
The index page's intro prose and card descriptions are prose surface; its `components:` front matter and any YAML fences stay byte-identical.

### D6: Validation without a full localized build in CI
Gate of record is `pnpm docs:check` (includes zh-tree checks). `npm run build` (full Docusaurus localized build) is run once at the end of the change to catch anything the static checks can't (e.g. MDX runtime errors in translated prose), since translated files are new MDX inputs.

## Risks / Trade-offs

- [MDX parse errors from untranslated Markdown syntax quirks (e.g. `{`, `<` in prose)] → keep all code spans fenced/inline-coded exactly as English; run `npm run build` once per D6.
- [Translation drift against future English edits] → accepted by policy (`DOCUMENTATION.md:101`); tracked by the follow-up freshness-report proposal.
- [Volume: 40 pages × review effort] → batched by component kind (D1) so review and gate failures stay scoped.

## Migration Plan

Additive-only: new files under `docs/i18n/zh-Hans/`, additive JSON entries. Rollback = delete the added files. No English content, routes, or gates change.

## Open Questions

(none)
