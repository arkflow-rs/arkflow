# Design: fix-mobile-navbar-drawer

## Context

`docs/src/css/custom.css` styles the navbar as a glass surface (rule at `.navbar`, ~line 707): `backdrop-filter: blur(12px) saturate(160%)` plus a translucent `color-mix` background and hairline border, with an `@supports not (backdrop-filter …)` fallback to a solid background. A CSS element with a non-`none` `backdrop-filter` becomes the containing block for `position: fixed` descendants. Docusaurus theme-classic renders the mobile sidebar (`navbar-sidebar`, stock `position: fixed; top: 0; bottom: 0`) as a child of `nav.navbar`, so at phone widths the drawer is sized against the 60px navbar box instead of the viewport (verified empirically; see proposal). The `nav` element itself carries no transform/filter — only the backdrop-filter triggers this.

Constraints: fix must live in our CSS (no swizzles, no upstream patches), keep the glass look pixel-identical in both themes, and keep the existing no-backdrop-filter fallback intact.

## Goals / Non-Goals

**Goals:**
- Restore the mobile drawer to full-viewport height with all entries visible.
- Keep the navbar's rendered glass surface unchanged (blur + saturation + translucent tint + hairline border, light and dark, desktop and mobile).
- Stay inside `docs/src/css/custom.css`.

**Non-Goals:**
- Navbar config changes (`hideOnScroll`), announcement bar mobile compaction, re-adding `.navbar__item`s to the mobile top bar (see proposal Non-goals).
- Any refactor of adjacent custom.css sections.

## Decisions

**D1 — Host the backdrop-filter on `.navbar::before`, not on `nav.navbar`.**
The pseudo-element (`content: ''; position: absolute; inset: 0; z-index: -1; backdrop-filter: <same value>`) is not an ancestor of the fixed drawer, so it cannot become the drawer's containing block; the nav's computed `transform` stays `none` and the drawer's fixed positioning resolves against the viewport again. Rendering order reproduces the current look: the pseudo blurs the page behind the bar and sits *below* the nav's own translucent background (z-index -1 within the nav's stacking context), so background tinting composites exactly as today. The `border-bottom` and translucent `background-color` stay on `.navbar` itself — backgrounds and borders never create containing blocks, and keeping them there preserves the `@supports` fallback shape.
*Alternatives considered:* (a) moving the effect to `.navbar__inner` (a child that doesn't contain the drawer) — rejected: narrows the glass region to the inner container and couples us to an Infima-owned element's geometry; (b) disabling glass below 996px — rejected: loses the visual language on mobile and leaves the same footgun latent on desktop; (c) removing the glass entirely — rejected: visual regression.

**D2 — Rely on the nav's existing stacking context for `z-index: -1`.**
`nav.navbar` is a positioned element (`position: sticky`/`--fixed-top`) with Infima's `z-index: var(--ifm-z-index-fixed)` (computed 200), so it already creates a stacking context; the negative-z pseudo is trapped inside it and cannot sink behind page content.

**D3 — Leave the `@supports not (backdrop-filter …)` block as is.**
In engines without backdrop-filter the pseudo's declaration is inert (no effect, no paint), and the solid-background fallback on `.navbar` continues to apply — no second `@supports` wrapper needed.

## Risks / Trade-offs

- [Pseudo-element backdrop-filter behaves differently in some WebKit versions] → The bar keeps its 92%-opaque background tint either way, so a lost blur degrades to "slightly flatter glass", never to unreadable; verification step covers Chromium; if Safari check is unavailable, the fallback risk is cosmetic only.
- [Future code re-adds `transform`/`filter`/`contain` on `.navbar` and silently re-clips the drawer] → The new delta-spec scenario ("drawer covers the viewport") plus the tasks' visual-verification step document the invariant; the comment in custom.css will call out the containing-block constraint so the next editor doesn't reintroduce it.
- [`z-index: -1` pseudo escaping the stacking context] → Not possible while the nav stays positioned with a z-index; D2 notes the dependency.

## Migration Plan

Single CSS edit in `docs/src/css/custom.css`; no build/config/data changes. Rollback = revert the commit. Verification: dev server at 375×667 (drawer full height, entries visible; glass bar visually unchanged in light + dark), desktop >996px unchanged, then `pnpm docs:check` and `pnpm build`.

## Open Questions

None.
