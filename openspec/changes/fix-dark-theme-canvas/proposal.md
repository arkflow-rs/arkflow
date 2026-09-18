## Why

Dark theme is broken below the first viewport on every docs site page. Two facts stack up:

1. `docs/src/css/custom.css:175` tries to darken the canvas with `[data-theme='dark'] html`, but Docusaurus sets `data-theme="dark"` **on** `<html>`, so the descendant selector never matches and `html` keeps the unconditional light-mode `background-color: #ffffff` from `custom.css:138`.
2. Infima's base stylesheet pins `html, body { height: 100% }`, so `body`'s dark background box (set by the working `[data-theme='dark'] body` half of the same rule) covers only one viewport height. Everything below it that does not paint its own background shows the white canvas.

Observed in a real browser (dev server, `data-theme=dark` confirmed, body box measured at 720px against a 3958px document): the landing page Features and "Where do you want to go" sections render white-on-white; docs page content columns (e.g. `/docs/get-started/quickstart`) render dark-mode light text on the white canvas below ~700px; macOS overscroll rubber-banding flashes white. Light mode works only by coincidence — the white canvas happens to be the light design.

## What Changes

- Darken the page canvas in dark mode by applying the background on a selector that actually matches `<html>` (e.g. inside the existing `[data-theme='dark']` token block at `custom.css:91`, or `html[data-theme='dark']`), and remove the dead `[data-theme='dark'] html` selector.
- No component, layout, or content changes; the top-anchored radial washes on `body` keep their current design behavior.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `docs-visual-system`: add the requirement that the page canvas stays dark at any scroll depth in dark mode (theme tokens resolve on the root element; no white exposure below the fold, on short pages, or during overscroll), in both themes.

## Impact

- `docs/src/css/custom.css` — one selector/background-color fix, both themes reviewed.
- Every docs site page in dark mode (landing page, docs pages, generated inventory page, 404).
- No CI gate behavior changes (`pnpm docs:check`, docs build, Rust docs-validation tests are untouched).

## Non-goals

- No redesign of the dark palette, gradients, or depth tokens.
- No changes to the "always dark" landing sections (hero, architecture, community) or light-mode appearance.
- No new screenshot/visual-regression CI infrastructure; verification is manual against the running dev server plus existing gates.
- No changes to Mermaid, search modal, or swizzled components — already verified theme-correct.
