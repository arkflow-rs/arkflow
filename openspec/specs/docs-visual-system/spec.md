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
