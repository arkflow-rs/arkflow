## Why

The documentation site is English-only while a meaningful share of ArkFlow's community reads Chinese: the repo already maintains a Chinese README (`README_zh.md`), yet the docs site hard-codes a single locale (`docs/docusaurus.config.ts:50-53`, `locales: ['en']`) and every validation gate scans only the English tree (`docs/scripts/docs-check.mjs:11`, `docsRoot = docs/`). Adding a Simplified Chinese locale as a progressive translation layer lets Chinese-speaking users onboard in their own language without forking the docs into a second site or slowing down English-first development.

## What Changes

- **English stays canonical.** `en` remains the default locale served at `/`; Simplified Chinese (`zh-Hans`) is served at `/zh-Hans/` as a pure translation layer. Locale switcher appears automatically; hreflang/`html lang` handled by Docusaurus.
- **i18n infrastructure**: register `zh-Hans` in `docusaurus.config.ts`, translate the site chrome (navbar, footer, announcement bar, tagline via code/theme JSON translations), and localize the landing page (`docs/src/pages/index.tsx`) with `<Translate>`.
- **Search**: enable Chinese tokenization in `@easyops-cn/docusaurus-search-local` (`language: ['en', 'zh']`) so both locales get working search.
- **First-mile translation (~15 pages)**: `get-started/` (3), `getting-started/` (2), `concepts/` (7), and the core build-concept pages in `build/` (jobs, streams, architecture). Untranslated pages fall back to English automatically.
- **Quality gates extended to the localized tree**: `docs:check` additionally validates `docs/i18n/zh-Hans/` — front-matter parity with the English counterpart (`id`, `slug`, `sidebar_position`, `components:`), internal links/anchors within the zh tree, and preservation of YAML fence classification markers.
- **Drift policy documented** in `docs/DOCUMENTATION.md`: English is the single upstream; translations may lag; no staleness enforcement in this change.

## Capabilities

### New Capabilities

- `documentation-i18n`: The site's localization layer — locale registration (`zh-Hans` as secondary locale with fallback-to-English semantics), translated site chrome, first-mile page coverage, and the English-canonical drift policy (what must stay in sync, what may lag, what is never translated).

### Modified Capabilities

- `documentation-quality-gates`: The documented validation command must also cover the localized tree — translated pages MUST keep front-matter parity with their English source, MUST NOT break internal links/anchors in the localized tree, and MUST preserve YAML fence classification markers; violations fail the same gate as English-tree violations.

## Impact

- **`docs/docusaurus.config.ts`** — `i18n.locales` gains `zh-Hans`; search plugin gains `language: ['en', 'zh']`.
- **`docs/i18n/zh-Hans/`** — new tree: `code.json` (config-level strings), theme-classic JSONs (navbar/footer), `docusaurus-plugin-content-docs/current/…` (translated pages), `docusaurus-plugin-content-pages/…` if the landing page needs a localized copy instead of `<Translate>`.
- **`docs/src/pages/index.tsx`** — strings wrapped in `<Translate>` (landing page is ~1047 lines; mechanical wrapping, no layout change).
- **`docs/scripts/docs-check.mjs`** — new section walking the zh tree; reuses existing slug/link/anchor helpers.
- **`docs/DOCUMENTATION.md`** — new i18n/drift-policy section.
- **No Rust changes** — the workspace snippet validator (`crates/arkflow/tests/docs_snippets_validate.rs`) keeps reading `docs/docs/` only; translated pages must not alter YAML blocks, which the zh-tree fence check enforces. Docs CI stays Node-only.
- **Versioned docs & blog are never translated** (see Non-goals); switching version or reading the blog under `zh-Hans` renders English fallback content.

## Non-goals

- Translating versioned documentation trees (`versioned_docs/`), blog posts, or the Tier-2/3 reference content (components/, sql/, reference/, operate/, how-to/, …) — deferred until the first-mile translation is stable.
- A CI staleness report for out-of-date translations — with ~15 translated pages, review discipline suffices; revisit when coverage passes ~50 pages.
- Machine-translation pipelines, Crowdin/translation-platform integration, or locale-specific theming.
- Changing the default locale, moving `en` off the site root, or altering URL structure of existing English routes.
