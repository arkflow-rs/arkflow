## 1. Layer 1 — Design tokens and global surfaces (CSS only)

- [x] 1.1 Define `--af-*` token set in `docs/src/css/custom.css` (gradient, glow levels, radii, motion durations, terminal accent) with complete light and dark values; keep existing `--af-card-*`/`--af-band-*` consumers working
- [x] 1.2 Add dark-mode radial depth background and light-mode equivalent canvas treatment to the doc page wrapper, with solid fallback where gradients unsupported
- [x] 1.3 Restyle display headings (`h1` doc titles, hub heroes) with the brand gradient text treatment; body text stays solid; verify AA contrast in both themes
- [x] 1.4 Terminalize code blocks with CSS only: title bar with window dots via `.theme-code-block` pseudo-elements, frame radius/border from tokens; verify titled and untitled blocks
- [x] 1.5 Restyle admonitions (gradient accent bar + icon chip) and markdown tables (mono field names, hover tint, softened stripes)
- [x] 1.6 Apply glass treatment to navbar and sidebar surfaces with `@supports` fallback; add brand-colored active-item indicator to sidebar
- [x] 1.7 Layer 1 verification: screenshot pass over get-started/concepts/components/reference pages in light AND dark, `pnpm docs:check` and `pnpm build` green

## 2. Layer 2 — Wayfinding affordances (Wrapping swizzles)

- [x] 2.1 Swizzle `TOCItems` (Wrapping) to add the scroll progress fill on the right-hand TOC, reusing Docusaurus scroll-spy state; rAF-throttled, instant state under `prefers-reduced-motion`
- [x] 2.2 Add a reading progress bar to doc pages (Wrapping swizzle of `DocPage/Layout` or equivalent), gated on reduced motion
- [x] 2.3 Layer 2 verification: progress affordances track correctly on a long page (e.g. `build/wal.md`), stock TOC click/navigation behavior intact, `pnpm typecheck` and `pnpm build` green

## 3. Layer 3 — Decoration kit and pipeline header

- [x] 3.1 Create `docs/src/components/` kit: `KindBadge`, `FeatureBadge`, `StatusDot` with both-theme styles from tokens; mono type, ≤200ms transitions
- [x] 3.2 Build `PipelineHeader` component: parse `kind/name` from front matter, render type name + topology role strip with subtle flow animation (disabled under reduced motion)
- [x] 3.3 Swizzle `DocItem/Header` (Wrapping) to render `PipelineHeader` above the title only when `frontMatter.components` exists; delegate all stock header behavior otherwise
- [x] 3.4 Verify pipeline header on representative pages (input/buffer/processor/output/codec, one each) and absence on non-component pages; check a versioned snapshot page still renders
- [x] 3.5 Layer 3 verification: `pnpm docs:check`, `pnpm build`, `pnpm typecheck` green; light+dark screenshot pass on component pages

## 4. Layer 4 — Hub pages and polish

- [x] 4.1 Restyle docs home hub (`docs/docs/index.md`) card grid using the token system: glass cards, kind badges on component cards, hover glow (reuse existing `.af-cards` classes)
- [x] 4.2 Restyle components gallery (`docs/docs/components.md`) into the kind-badged card gallery; keep all existing links and front matter contracts intact
- [x] 4.3 Decide and apply docs-home hero motion (default: keep current animated code window as the only hero motion)
- [x] 4.4 Layer 4 verification: internal-link/anchor validation passes, `pnpm docs:check` green, light+dark screenshot pass on both hub pages

## 5. Final acceptance

- [x] 5.1 Full-site reduced-motion audit: with `prefers-reduced-motion` enabled, no looping/transition animations run anywhere in docs (landing page pattern held)
- [x] 5.2 Full CI parity check: `pnpm docs:check`, lychee link check, and Rust docs validation tests (`cargo test -p arkflow --test docs_snippets_validate` and workspace doc tests) all pass with gate scripts and data files unchanged
- [x] 5.3 Docusaurus upgrade-safety review: confirm all swizzles in `docs/src/theme/` are Wrapping, delegate to originals, and each is <100 lines
- [x] 5.4 Run the archived-change checklist: update this change's artifacts if any Open Questions from design.md were resolved during implementation
