## ADDED Requirements

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
