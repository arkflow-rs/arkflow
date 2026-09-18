## Context

The #1234 visual system layered pipeline headers, tables styling, progress bars, and a terminal code frame on top of stock Docusaurus 3.9.2, with all styling centralized in `docs/src/css/custom.css` and three wrapping swizzles. None of it was designed against a phone viewport: the pipeline role strip (`custom.css` `.af-pipeline__strip`) is a non-wrapping inline-flex with ≈360px min-content width; markdown tables got `display: table; width: 100%` with no scroll container anywhere in the stack; mermaid keeps the `useMaxWidth: true` default, scaling wide diagrams to viewport instead of allowing scroll. The landing page (`docs/src/pages/`) already demonstrates the correct pattern — breakpoint collapse plus a dedicated horizontal scroll wrapper (`.archScroll`) with a scroll hint — so this change brings the docs reading surfaces up to the standard the landing page already meets. The `docs-visual-system` spec (`openspec/specs/docs-visual-system/spec.md`) has no mobile scenarios, which is why the gap shipped.

Constraints: CI gates (`pnpm docs:check`, example validation, inventory generation, lychee) must pass with their scripts and data files unchanged; swizzles must stay small and Wrapping-only; `--af-*` tokens must degrade to stock Infima styling; reduced-motion behavior must be preserved.

## Goals / Non-Goals

**Goals:**

- No page-level horizontal scroll on any docs site page at 360–414px viewports, in both themes.
- Wide content (config tables, inventory table, mermaid flowcharts) scrolls within its own container at readable size instead of shrinking or overflowing.
- Desktop rendering (>996px) stays pixel-identical, except wide mermaid diagrams which swap shrink-to-fit for scroll (see D3).
- Spec amendment so mobile viewports become an enforced requirement of `docs-visual-system`, not a one-off fix.

**Non-Goals:**

- No automated Playwright/mobile-overflow CI gate in this change — the acceptance bar is defined as spec scenarios and verified manually per the tasks; adding browser infra to docs CI is a follow-up if regressions recur.
- No changes to the landing page, 404 page, search modal, navbar, sidebar drawers, or `.af-cards` grids (all already mobile-correct).
- No content rewrites (splitting the inventory table, shortening config tables) — layout fix only.
- No `viewport-fit=cover` / PWA notched-display work beyond the safe-area one-liner in D4.
- No visual-token or palette changes; dark/light values untouched.

## Decisions

### D1: Tables become self-scrolling blocks below 996px

`@media (max-width: 996px) { .markdown table { display: block; width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; } }`.

- Why 996px: it is Docusaurus's own breakpoint where the sidebar collapses, so the rule engages exactly when the content column stops being desktop-sized; above it, desktop rendering is untouched.
- Alternatives considered: (a) always block-level — rejected, it changes narrow-table stripe width on desktop, violating the pixel-identical goal; (b) `overflow-wrap: anywhere` on cells only — rejected, wrapping identifiers mid-token makes config field names unreadable and still fails on 4–5 column tables; (c) wrapping tables in a scroll `div` via a swizzle — rejected, content-level DOM changes for layout alone are not worth a swizzle.
- Known cosmetic cost: below 996px, a narrow table's zebra stripes stop spanning the full container (row backgrounds track the inner table box, not the block). Accepted: on phones the overflowing tables — the reason this rule exists — are full-width anyway.

### D2: Pipeline role strip wraps; connectors hide on ≤640px

Inside a `@media (max-width: 640px)` block: `.af-pipeline__strip { flex-wrap: wrap; row-gap: 0.35rem; margin-left: 0; }` and `.af-pipeline__link { display: none; }`.

- The strip keeps all four stages visible (its purpose is showing pipeline position) but stacks the chips in reading order instead of overflowing the rounded container.
- Alternatives: (a) render only the active role chip on mobile — hides information the header exists to convey; (b) make the pill itself `overflow-x: auto` — nested horizontal scrolling on phones is poor UX and hides the overflow behind a scrollbar most users won't notice.
- 640px (not 996px) because the strip only breaks below ~420px content width; keeping the one-line arrow strip on tablets preserves the nicer rendering wherever it fits.

### D3: Mermaid renders at natural width and scrolls (sitewide)

Add `options: { flowchart: { useMaxWidth: false } }` to the preset's `mermaid` themeConfig and give `.mermaid { overflow-x: auto; }` in `custom.css`.

- Readability-first: a `flowchart LR` scaled to 335px renders ~4px text — functionally absent; at natural width it is fully legible and scrolls, matching the landing page's architecture-diagram UX.
- Desktop trade-off, accepted: a diagram wider than the content column now scrolls instead of shrinking. For the two existing diagrams this is neutral-to-better on desktop; the alternative (scaling on desktop, scrolling on mobile) is impossible without a JS resize re-render of mermaid, which is disproportionate.
- Config lives in `docs/docusaurus.config.ts` (the only non-CSS file touched).

### D4: Reading-progress bar respects safe-area insets

`.af-reading-progress { top: env(safe-area-inset-top, 0); }`. A no-op in ordinary browsers (insets are 0 without `viewport-fit=cover`) and correct if standalone/notched contexts arrive later. One line; no `viewport-fit` change in this scope.

### D5: Spec amendment over new capability

Mobile usability is added as a requirement of the existing `docs-visual-system` capability (ADDED scenarios: no page-level horizontal scroll at 360/390px in both themes; pipeline header wraps; tables scroll in place; mermaid scrolls at natural size) rather than a new capability — it is the same visual system under a narrower viewport, and the capability already owns the surfaces involved.

## Risks / Trade-offs

- [Block tables change narrow-table stripe width below 996px] → Cosmetic only; documented in D1, verified visually in the acceptance pass.
- [Wide mermaid diagrams scroll on desktop instead of shrinking] → Accepted readability-first trade-off (D3); revisit only if a diagram becomes unusably tall-and-wide.
- [`useMaxWidth: false` mis-keyed in themeConfig would silently do nothing] → Verified by rendering `develop/kernel.md` at 390px during acceptance.
- [Future wide content reintroduces page-level overflow] → Spec scenario now forbids it; if it regresses, the follow-up Playwright gate is the escalation path (Non-Goal today).
- [Reduced-motion regression] → None expected: no new animations are added; wrap/hide are static layout changes.

## Migration Plan

Single docs-site commit; no build flags, no data migration, no Rust rebuild. Deploy is the normal docs publish. Rollback is reverting the commit. CI gates are untouched and must pass unchanged (`pnpm docs:check`, `pnpm docs:build`, workspace docs tests) before merge.

## Open Questions

- None. The mermaid direction (the only real fork) is decided in D3; the CI gate question is explicitly deferred as a Non-Goal.
