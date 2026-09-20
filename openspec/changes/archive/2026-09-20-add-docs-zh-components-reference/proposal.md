## Why

The `zh-Hans` locale shipped in `add-docs-i18n-zh` covers only the first mile (16 of 128 pages, per `docs/DOCUMENTATION.md:100`); everything else falls back to English under `/zh-Hans/`. The deferred Tier-2/3 content starts with the components reference — `docs/docs/components/` holds 39 component pages plus the `docs/docs/components.md` section index, and none of them exist in `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/`. This is the highest-value documentation for plugin users (every input/buffer/processor/output/codec, 85 `yaml` example fences), and the first-mile set it depends on has shipped and stabilized (merged as #1240), which is exactly the condition the previous proposal set for continuing. Until this lands, a Chinese reader configuring any plugin reads English fallback content at localized routes.

## What Changes

- **Translate the full components reference into the `zh-Hans` tree** — all 39 pages under `docs/docs/components/` (`0-inputs/` 13, `3-outputs/` 11, `2-processors/` 6, `5-codecs/` 4, `1-buffers/` 4, `4-temporary/` 1) plus the `docs/docs/components.md` section index, mirrored at `docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/`.
- **Translation surface stays prose-only**: titles, descriptions, headings, table cells, and prose are localized; front-matter identity (`id`, `slug`, `sidebar_position`, `sidebar_label`), the `components:` ownership lists, and every ` ```yaml ` fence (classification metastring + content, 85 fences) remain byte-identical to English — already enforced by `pnpm docs:check`.
- **Sidebar category labels** for the components section (`_category_.json` labels such as Inputs/Outputs/Processors/Buffers/Codecs) are localized through the docs-plugin i18n JSON so the localized sidebar renders in Chinese.
- **Linking rules respected**: translated pages use file-relative links only to targets that exist in the zh tree; everything else uses locale-prefixed absolute routes (e.g. `[配置参考](/zh-Hans/docs/reference/configuration)`).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `documentation-i18n`: adds a coverage requirement — the components reference section SHALL be translated, extending the existing first-mile coverage requirement; the structural-fidelity and English-canonical rules are unchanged and keep referring to `documentation-quality-gates` for enforcement.

## Impact

- **`docs/i18n/zh-Hans/docusaurus-plugin-content-docs/current/components/**`** — new translated tree (~40 files), plus the components sidebar-category labels in the docs-plugin i18n JSON.
- **`pnpm docs:check`** — must pass with the zh tree added; no script changes expected (the zh-tree checks from the previous change apply as-is).
- **No Rust changes** — component YAML examples are never modified (byte-identical fences), so the workspace snippet validator keeps reading only `docs/docs/`.
- **READMEs unaffected** — component lists remain English-only per policy.

## Non-goals

- Translating the remaining Tier-2/3 sections (`build/` beyond the core pages, `sql/`, `reference/`, `operate/`, `how-to/`, `control-plane/`, `develop/`, `cases/`, root pages) — handled by follow-up proposals so each lands independently.
- A translation freshness/staleness report — separate follow-up proposal (coverage will exceed the ~50-page revisit threshold once the reference sections land).
- Versioned docs and blog posts — never translated per the documented policy.
- Machine-translation pipelines, Crowdin integration, or locale-specific theming.
- Changing validation gates, the default locale, or existing English routes/content.
