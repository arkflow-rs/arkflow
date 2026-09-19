# Proposal: fix-mobile-navbar-drawer

## Why

On phone viewports the docs site's mobile navigation drawer is unusable: tapping the hamburger button renders a ~60px-tall strip instead of a full-height menu, so every drawer entry (docs menu, versions, theme toggle) is clipped out of view. Root cause (verified live at a 375×667 viewport against the dev server): `docs/src/css/custom.css:708` applies `backdrop-filter: blur(12px) saturate(160%)` to `.navbar`, and per the CSS spec any element with a non-`none` `backdrop-filter` becomes the containing block for `position: fixed` descendants. Docusaurus renders the mobile sidebar (`navbar-sidebar`, stock `position: fixed; top: 0; bottom: 0`) inside `<nav class="navbar">` (theme-classic `Navbar/Layout/index.tsx`), so the drawer's viewport reference is the 60px navbar box itself. A runtime diagnostic (removing only `backdrop-filter` in-page) restored the drawer to full viewport height with all menu entries visible, confirming the causal chain.

This regressed with the visual system modernization (#1234), which introduced the glass navbar. The `@supports not (backdrop-filter …)` fallback in `custom.css` does not help: precisely the browsers that support `backdrop-filter` — i.e. all modern ones — exhibit the containing-block behavior.

## What Changes

- Move the navbar's glass effect (backdrop blur/saturation) off the `nav.navbar` element onto a pseudo-element layer (`.navbar::before`) that is not an ancestor of the fixed mobile sidebar, so the drawer's containing block is the viewport again.
- Keep the visual result unchanged: the top bar still renders the glass surface (blur + saturation + translucent background + hairline border) in both light and dark themes, and the `@supports not (backdrop-filter …)` solid-background fallback still applies.
- No changes to navbar configuration (`hideOnScroll` stays as is), no changes to the announcement bar, no changes to any theme components or Infima/Docusaurus internals.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

(none — the behavior fix lands as a new requirement added to the existing `docs-visual-system` capability)

- `docs-visual-system`: ADD a requirement "The mobile navigation drawer SHALL be fully usable" (the navbar surface effect must not clip the fixed-positioned drawer; drawer covers the viewport at phone widths; glass visuals unchanged). No existing requirement text is modified.

## Non-goals

- **Removing or scoping `hideOnScroll: true`** (`docs/docusaurus.config.ts:112`): hiding the navbar on scroll-down is a deliberate reading optimization and stays; the drawer fix restores the navigation entry point (scroll up reveals the bar, then the hamburger works).
- **Compacting the announcement bar on mobile**: at 375px it wraps to ~4 lines (114px) before the navbar. Worth addressing, but it is a first-screen space concern, not a navigation-availability bug — a separate change if wanted.
- **Restoring version dropdown / GitHub / color toggle into the mobile top bar**: Infima intentionally hides `.navbar__item`s below 996px and the drawer carries those entries; fixing the drawer makes them reachable again.
- **Any change to theme components, Docusaurus internals, or Rust code.**

## Impact

- **Files**: `docs/src/css/custom.css` only (the `.navbar` glass rule and its `@supports` fallback block).
- **Verification**: dev-server visual check at 375×667 (drawer opens full height, glass bar renders unchanged), plus existing gates: `pnpm docs:check` and `pnpm build` in `docs/`.
- **Risk**: low — CSS-only, desktop rendering path is the same rule restated; the only behavior-sensitive consumer of `.navbar` geometry is the fixed mobile sidebar, which this change unbreaks.
