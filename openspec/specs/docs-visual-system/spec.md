# Purpose

Define the visual design language of the ArkFlow documentation site: the tokenized theme system (palette, gradient, depth, motion), reading-surface styling, front-matter-driven component decorations, wayfinding affordances, and the constraints that keep the system compatible with Docusaurus upgrades and the documentation quality gates.

## Requirements

### Requirement: Design tokens SHALL define the site visual system
The docs site SHALL define its visual design language as CSS custom properties (namespaced `--af-*`) covering brand gradient, glow/depth levels, radii, and motion durations, with a complete value set for both light and dark themes. The tokens MUST compose on top of Infima `--ifm-*` variables without replacing Docusaurus's theme-switching mechanism.

#### Scenario: Theme toggle switches the visual system
- **WHEN** a user toggles between light and dark themes on any docs page
- **THEN** the gradient, background depth, surface, and glow tokens resolve to their theme-specific values without unstyled flashes or broken contrast

#### Scenario: Tokens degrade without breaking stock styling
- **WHEN** any `--af-*` token is removed from the stylesheet
- **THEN** the affected element falls back to its Infima/default styling rather than rendering without background, border, or shadow

### Requirement: Component pages SHALL render a pipeline header derived from front matter
Doc pages whose front matter declares `components: [<kind>/<name>]` SHALL render a pipeline header above the page title showing the component's type name and its role in the stream topology (input, buffer, processor, output, temporary, codec), built from the existing front matter without requiring per-page content edits. Pages without `components:` front matter SHALL render no pipeline header.

#### Scenario: Component page shows its pipeline position
- **WHEN** a user opens `docs/docs/components/0-inputs/kafka.md` (front matter `components: [input/kafka]`)
- **THEN** the page renders a pipeline header identifying `input/kafka` and its role as an input before the page title

#### Scenario: Non-component page is unaffected
- **WHEN** a user opens a docs page without `components:` front matter (e.g. `docs/docs/build/streams.md`)
- **THEN** the page renders no pipeline header and its title layout is unchanged

#### Scenario: New component page gets the decoration for free
- **WHEN** a contributor adds a new component page following the existing `components:` front-matter convention
- **THEN** the pipeline header renders without any additional per-page markup or registration step

### Requirement: Reading surfaces SHALL carry the visual system
Doc reading surfaces SHALL be restyled within the token system: display headings carry the brand gradient treatment, code blocks render inside a terminal-style frame with a title bar, admonitions use a gradient accent bar, and configuration tables use monospace field styling. Restyling MUST apply in both light and dark themes.

#### Scenario: Code block renders as a terminal window
- **WHEN** a user views any fenced code block on a docs page
- **THEN** the block is framed with a terminal-style title bar (window dots plus the block title when present) in the current theme

#### Scenario: Code blocks without titles render correctly
- **WHEN** a code block has no `title` meta string
- **THEN** the terminal frame renders with the title bar dots and no broken or empty title area

### Requirement: Navigation SHALL provide progress affordances
The docs reading experience SHALL include scroll-aware wayfinding: the sidebar active item carries a brand-colored indicator, the right-hand table of contents shows a progress fill tracking scroll position, and doc pages show a reading progress bar. These affordances MUST reuse Docusaurus's existing scroll-spy state rather than replacing it.

#### Scenario: TOC progress tracks reading position
- **WHEN** a user scrolls through a long docs page
- **THEN** the TOC progress fill advances with the scroll position and the active heading indicator follows the section in view

#### Scenario: Sidebar highlights the current page
- **WHEN** a user is on a docs page
- **THEN** its sidebar entry carries the brand-colored active indicator in the current theme

### Requirement: Motion SHALL respect the reduced-motion preference
All animation introduced by the visual system (gradient shifts, pipeline flow loops, glow transitions, progress fills) SHALL be disabled or reduced to instantaneous state changes when the user's `prefers-reduced-motion` preference is set, following the pattern already used by the landing page.

#### Scenario: Reduced-motion user gets static rendering
- **WHEN** the OS-level `prefers-reduced-motion` setting is enabled
- **THEN** no looping or transition animations run on docs pages and all state changes (active items, progress fills) apply instantly

### Requirement: Swizzled theme components SHALL remain upgrade-compatible
Customizations of Docusaurus theme components SHALL use Wrapping swizzles that delegate to the original component implementation; Ejecting swizzles MUST NOT be introduced. Each swizzle MUST remain small enough that a Docusaurus minor upgrade diff can be rebased manually.

#### Scenario: Swizzle delegates to stock behavior
- **WHEN** a doc page renders through a swizzled theme component
- **THEN** all stock Docusaurus behavior (layout, metadata, versioning affordances) remains functional, with the visual system layered on top

### Requirement: The visual system SHALL NOT regress content CI gates
The visual system changes MUST NOT modify the behavior of documentation quality gates: `pnpm docs:check`, example manifest validation, component inventory generation, and link checking SHALL pass without script changes.

#### Scenario: CI gates pass after restyling
- **WHEN** a visual-system layer is merged
- **THEN** `pnpm docs:check`, the lychee link check, and the Rust inventory/docs validation tests pass with the gates' scripts and data files unchanged

#### Scenario: Hub page rewrites preserve contracts
- **WHEN** the docs home or components hub page is restyled
- **THEN** the page keeps its sidebar registration, internal links, and front-matter contracts so anchor and reachability validation still passes

### Requirement: The visual system SHALL remain usable at mobile viewports
The docs site SHALL render without page-level horizontal scroll at phone viewports (360px and 390px widths) in both light and dark themes. Wide content — markdown tables, mermaid diagrams, the component pipeline header — SHALL be contained by its own scroll or wrap behavior instead of expanding the page. This applies to every docs site page, including generated pages (component inventory) and the landing page.

#### Scenario: No page-level horizontal scroll on a phone
- **WHEN** any docs site page (e.g. `docs/docs/reference/component-inventory.md`, a component reference page, the docs home, or the landing page) is loaded at a 360px-wide viewport in either theme
- **THEN** the document's scroll width does not exceed the viewport width, and any overflow is confined to scroll containers inside the page (tables, diagrams)

#### Scenario: Pipeline header wraps on a phone
- **WHEN** a component reference page (front matter `components: [...]`) is viewed at a 360px-wide viewport
- **THEN** the pipeline role strip (INPUT → BUFFER → PROCESSOR → OUTPUT) stays inside the pill container by wrapping its stage chips, and no stage chip overflows or clips outside the container border

#### Scenario: Wide table scrolls within its own container
- **WHEN** a docs page contains a markdown table wider than the content column (e.g. the component inventory table) and it is viewed at a 360px-wide viewport
- **THEN** the table scrolls horizontally inside its own container while the rest of the page remains fixed, and no table cell content is clipped from view

#### Scenario: Wide mermaid diagram scrolls at natural size
- **WHEN** a docs page contains a mermaid flowchart wider than the content column (e.g. `docs/docs/develop/kernel.md`) and it is viewed at a 360px-wide viewport
- **THEN** the diagram renders at natural size with legible text and scrolls horizontally inside its own container instead of shrinking to an unreadable scale

#### Scenario: Desktop rendering is unchanged
- **WHEN** the same pages are viewed at a desktop viewport (>996px width)
- **THEN** tables, pipeline headers, and reading surfaces render as they did before this change, except mermaid diagrams wider than the content column, which scroll instead of scaling down

#### Scenario: Documentation quality gates still pass
- **WHEN** the mobile-compatibility CSS and mermaid config changes are merged
- **THEN** `pnpm docs:check`, `pnpm docs:build`, the lychee link check, and the Rust docs-validation tests pass with the gates' scripts and data files unchanged

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

### Requirement: The page canvas SHALL match the active theme at any scroll depth
The docs site root canvas (`html`) SHALL carry the active theme's background color so that every region of the page not painted by a specific element shows the theme canvas in both light and dark mode, regardless of scroll position, page length, or overscroll. The dark-theme override MUST select the root element itself (e.g. `html[data-theme='dark']` or the `[data-theme='dark']` token block) rather than a descendant selector, because Docusaurus sets `data-theme` on `<html>`. Content elements that paint their own backgrounds (cards, code blocks, bands, footer) MUST remain unchanged.

#### Scenario: Dark canvas below the first viewport on a docs page
- **WHEN** a user scrolls beyond the first viewport on any docs page in dark mode (e.g. `/docs/get-started/quickstart`)
- **THEN** the content column, sidebar, and table of contents areas show the dark canvas color (`--ifm-background-color`, `#0b1120`), and text remains light-on-dark and readable

#### Scenario: Dark canvas on the landing page
- **WHEN** a user scrolls through the landing page's transparent sections (Features, "Where do you want to go") in dark mode
- **THEN** the section background shows the dark canvas color rather than white, with section headings readable against it

#### Scenario: Overscroll does not flash white
- **WHEN** a user rubber-band overscrolls at the top or bottom of any page in dark mode
- **THEN** the exposed canvas area is the dark theme color, not white

#### Scenario: Light mode is unchanged
- **WHEN** the same pages are viewed in light mode
- **THEN** the canvas remains the light design (`#ffffff` with the top-anchored radial washes) exactly as before this fix

### Requirement: Reading surfaces SHALL carry finishing wayfinding and interaction affordances
Doc-page reading surfaces SHALL be finished within the token system: the prev/next pagination renders as brand cards, long doc pages offer a back-to-top affordance that respects the reduced-motion preference, blockquotes carry the quiet brand treatment, untitled code blocks identify their language in the terminal bar, the code-block copy button sits within the terminal bar, and breadcrumbs, heading hash-links, and markdown links use subdued affordance styling. All treatments MUST apply in both light and dark themes and degrade to stock Infima styling when `--af-*` tokens are removed.

#### Scenario: Pagination renders as brand cards
- **WHEN** a user reaches the bottom of a doc page that has both previous and next pagination links
- **THEN** both links render as cards with the token surface and radius, a muted sublabel over a semibold label, and a hover state with lift, glow, and border tint in the current theme

#### Scenario: Back-to-top appears after scrolling and respects reduced motion
- **WHEN** a user scrolls a long docs page past roughly one viewport height
- **THEN** a back-to-top control appears fixed at the bottom right, scrolls the page to the top when activated, and under `prefers-reduced-motion` performs the jump instantly instead of animating

#### Scenario: Blockquotes carry the quiet brand treatment
- **WHEN** a docs page renders blockquotes (e.g. the SQL reference function-signature pages)
- **THEN** each blockquote renders as a rounded band with the gradient accent bar and a subtle background tint in the current theme, visually quieter than an admonition

#### Scenario: Untitled code blocks identify their language
- **WHEN** a code block without a `title` meta string is rendered
- **THEN** the terminal bar shows the block's language as a monospace micro-label without colliding with the copy button, and blocks with titles are unchanged

#### Scenario: Reading surfaces carry the typographic and depth foundation
- **WHEN** any docs page is viewed in either theme
- **THEN** body text renders in the self-hosted Inter face with antialiased rendering, code blocks/cards/pagination/back-to-top carry the layered shadow tokens, and the announcement bar renders as a quiet deep-navy band

#### Scenario: Documentation quality gates still pass
- **WHEN** the finishing-affordance styles and the DocRoot swizzle addition are merged
- **THEN** `pnpm docs:check`, `pnpm typecheck`, `pnpm docs:build`, the lychee link check, and the Rust docs-validation tests pass with the gates' scripts and data files unchanged
