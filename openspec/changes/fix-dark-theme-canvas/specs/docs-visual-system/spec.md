## ADDED Requirements

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
