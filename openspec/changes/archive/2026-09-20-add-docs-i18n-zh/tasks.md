## 1. i18n infrastructure

- [x] 1.1 Register `zh-Hans` in `docs/docusaurus.config.ts` (`i18n.locales: ['en', 'zh-Hans']`, defaultLocale stays `en`) and create the `docs/i18n/zh-Hans/` skeleton (theme/plugin JSON dirs, `docusaurus-plugin-content-docs/current/`)
- [x] 1.2 Wrap the config-level strings (tagline, announcement bar) in `translate()` and add their Simplified Chinese messages to `i18n/zh-Hans/code.json`; verify the English site renders unchanged
- [x] 1.3 Generate and fill theme JSONs (`docusaurus-theme-classic/navbar.json`, `footer.json`) with Simplified Chinese labels via `docusaurus write-translations`
- [x] 1.4 Enable bilingual tokenization on `@easyops-cn/docusaurus-search-local` (`language: ['en', 'zh']`) and verify search works on both locale builds (CJK query on `/zh-Hans/`, English query on `/`)
- [x] 1.5 Run `pnpm build` and confirm both locales build; spot-check that `/zh-Hans/docs/<untranslated page>` serves English fallback content under localized chrome without 404

## 2. Localized-tree validation gate

- [x] 2.1 Extend `docs/scripts/docs-check.mjs` with a zh-tree walker: counterpart existence (mirrored path under `docs/docs/`) and front-matter parity (`id`, `slug`, `sidebar_position`, `sidebar_label`, `components:`) checks with file-level error reporting
- [x] 2.2 Add fallback-aware internal-link/anchor validation for zh pages (resolve against zh tree first, English tree when the target is untranslated)
- [x] 2.3 Add the YAML-fence byte-fidelity check (metastring + content must equal the English counterpart's corresponding fence)
- [x] 2.4 Verify the new checks with throwaway fixtures (orphan page, drifted front matter, broken link, edited YAML fence) and confirm each fails `pnpm docs:check` with the offending file named; remove fixtures afterwards

## 3. Landing page localization

- [x] 3.1 Copy `docs/src/pages/index.tsx` into `i18n/zh-Hans/docusaurus-plugin-content-pages/` and translate its user-facing strings; keep the English original untouched
- [x] 3.2 Build and inspect `/zh-Hans/` versus `/`: Chinese landing renders with layout/visuals intact, English landing unchanged

## 4. First-mile translations (~15 pages)

- [x] 4.1 Translate `docs/docs/get-started/` (3 pages) into `i18n/zh-Hans/docusaurus-plugin-content-docs/current/get-started/`
- [x] 4.2 Translate `docs/docs/getting-started/` (2 pages)
- [x] 4.3 Translate `docs/docs/concepts/` (7 pages)
- [x] 4.4 Translate the core build-concept pages `docs/docs/build/architecture.md`, `streams.md`, `jobs.md` (paths as found in the tree)
- [x] 4.5 Confirm all first-mile routes render Simplified Chinese under `/zh-Hans/docs/...`, and `pnpm docs:check` passes over the new tree

## 5. Policy and final verification

- [x] 5.1 Add the i18n/drift-policy section to `docs/DOCUMENTATION.md`: English is canonical, translations may lag, never-translate list (versioned trees, blog), YAML-fence and front-matter rules for translators, and how the zh-tree gate works
- [x] 5.2 Run the full gate set: `pnpm docs:check`, `pnpm build`, and `cargo test --workspace --all-targets` (snippet validator unchanged and green); record build-time impact of the dual-locale build
- [x] 5.3 Walk the locale switcher, version dropdown, and search on the built site across both locales and confirm no broken navigation between `/` and `/zh-Hans/` routes
