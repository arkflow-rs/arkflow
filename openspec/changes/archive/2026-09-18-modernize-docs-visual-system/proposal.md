## Why

The docs site reading experience is stock Docusaurus while the landing page already has a distinct visual personality — animated code reveal (`docs/src/pages/index.tsx:62`), gradient 404 (`docs/src/css/custom.css:233-266`), and card hover polish (`docs/src/css/custom.css:128-160`). The visual system itself is barely defined: the palette is default Tailwind blue-600 (`docs/src/css/custom.css:21-27`), all 128 content pages use the unmodified classic preset layout (`docs/docusaurus.config.ts:55-97`), and code highlighting is the stock github/dracula pair (`docs/docusaurus.config.ts:193-197`). The result is a site whose inner pages read as generic documentation, undercutting the product identity (a stream-processing engine) that the landing page establishes.

## What Changes

- Introduce an "Engine × Premium" hybrid visual language for the docs site: a Premium-SaaS quality base (depth, glow, glass surfaces, generous spacing, both themes first-class) carrying Engine/Terminal identity motifs (flowing pipeline decorations, terminal-style code blocks, monospace badges/status dots).
- Define reusable design tokens (color gradients, radii, depth/glow, motion durations) in `docs/src/css/custom.css`, with dark mode (`#0b1120` deep space) as lead theme and an equal-quality light theme.
- Restyle global reading surfaces: headings with flow-gradient treatment, terminalized code blocks, admonitions, tables, sidebar active-item glow, glass navbar.
- Add scroll/reading affordances via light (Wrapping) swizzles: TOC scroll-spy progress line, reading progress bar.
- Add an MDX component kit: `PipelineHeader` (auto-derived from existing `components:` front matter), `KindBadge`, `FeatureBadge`, `StatusDot` — the pipeline header renders on all 39 component pages without per-page hand editing.
- Rework the docs home / hub pages (`docs/docs/index.md`, `docs/docs/components.md`) into a card gallery with kind badges.
- All motion respects `prefers-reduced-motion`; swizzling is Wrapping-only to preserve the Docusaurus upgrade path.

## Capabilities

### New Capabilities

- `docs-visual-system`: The visual design language of the docs site — design tokens (palette, gradient, depth, motion), dual-theme requirements, reading-surface styling, swizzled wayfinding affordances, the MDX decoration component kit, and front-matter-driven pipeline rendering on component pages.

### Modified Capabilities

None. The `documentation-quality-gates` requirements (link checks, example validation, inventory freshness) are unchanged; this change must not regress them, but no spec-level behavior of existing capabilities changes.

## Impact

- `docs/src/css/custom.css` — tokens and global restyling (main surface).
- `docs/src/theme/` — new Wrapping swizzles (TOC progress, reading progress); joins existing `NotFound.tsx`.
- `docs/src/components/` — new MDX decoration kit; consumed by MDX pages via imports, and by the component-doc pipeline header.
- `docs/src/pages/index.module.css`, hub pages — gallery/badge restyling.
- `docs/docusaurus.config.ts` — possibly prism theme pair swap only; no structural config changes.
- No Rust code, no CI gate scripts (`docs-check.mjs`, lychee, inventory generation) — they must keep passing unmodified.
- New npm dependencies: none required (CSS + existing React/Docusaurus APIs).

## Non-goals

- No docs framework migration (Fumadocs/Nextra/Starlight) — CI gates (`docs:check`, inventory validation, link checking) assume Docusaurus structure.
- No content rewrites or information-architecture changes (see `documentation-information-architecture` spec) — visuals only, page URLs and front matter contracts stay intact.
- No changes to the console (`console/`) or control-plane UIs.
- No dark-mode-only shipping: light and dark themes land together.
- No custom illustrations/brand artwork production — the system uses CSS/SVG-only decoration.
