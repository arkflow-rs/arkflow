## ADDED Requirements

### Requirement: The mobile navigation drawer SHALL be fully usable at phone viewports
The docs site's mobile navigation drawer (Docusaurus `navbar-sidebar`) SHALL cover the full viewport height when opened at phone widths, with its navigation entries (docs menu, versions, theme toggle, external links) visible and tappable. The navbar surface MUST NOT become the containing block for the fixed-positioned drawer: any paint-affecting property that establishes a containing block for `position: fixed` descendants (e.g. `backdrop-filter`, `filter`, `transform`) MUST be hosted on a layer that is not an ancestor of the drawer (e.g. a navbar pseudo-element). The navbar's rendered glass surface SHALL remain unchanged in both themes at all viewports.

#### Scenario: Drawer opens full height on a phone
- **WHEN** the hamburger button ("Toggle navigation bar") is tapped on any docs page at a phone viewport (e.g. 360px or 375px width)
- **THEN** the drawer spans the full viewport height from the top of the page, the close button and the navigation entries (Docs, SQL, API, Blog, version selector, docs sidebar sections) are visible and tappable, and no entry is clipped to the navbar's own height

#### Scenario: Navbar glass surface is unchanged
- **WHEN** any docs page is viewed at desktop (>996px) or phone width, in light and dark themes
- **THEN** the top bar renders the same glass surface as before this change — translucent tint over blurred page content with the hairline bottom border — and no element behind or beside the navbar changes appearance

#### Scenario: No-backdrop-filter fallback is unchanged
- **WHEN** the site is rendered by an engine that does not support `backdrop-filter`
- **THEN** the navbar falls back to the solid background color exactly as before, and the drawer remains full-height

#### Scenario: Documentation gates still pass
- **WHEN** the navbar CSS change is merged
- **THEN** `pnpm docs:check` and `pnpm build` pass in `docs/` with their scripts and data files unchanged
