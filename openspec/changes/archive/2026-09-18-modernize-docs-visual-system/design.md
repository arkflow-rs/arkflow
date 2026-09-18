## Context

The docs site is Docusaurus 3.9.2 (`docs/package.json`) on the classic preset with React 19, local search, mermaid, and versioned docs. Customization so far is limited to the landing page (`docs/src/pages/index.tsx` — animated code window, tilt), a custom 404 (`docs/src/theme/NotFound.tsx`), and ~300 lines of restrained CSS (`docs/src/css/custom.css`). All 128 content pages render through the stock doc layout.

The site already carries CI-validated content contracts this change must not disturb: `components:` front matter on all 39 component pages (validated by `docs/scripts/docs-check.mjs` against the generated inventory), example manifest validation, lychee link checking, and internal-link anchor validation (see the `documentation-quality-gates` spec).

Design direction (agreed during exploration): a hybrid visual language — Premium-SaaS quality base (depth, glow, glass, spacing, dual themes) carrying Engine/Terminal identity motifs (pipeline decorations, terminal code blocks, mono badges). Rationale: pure "Engine" style risks feeling niche and makes the light theme an afterthought; pure "Premium" style is indistinct. The hybrid extends the visual language the landing page already established.

## Goals / Non-Goals

**Goals:**

- A tokenized visual system (palette, gradient, depth, motion) living in CSS custom properties, coexisting with Infima variables.
- Reading pages that feel designed: gradient headings, terminalized code frames, restyled admonitions/tables, glass navbar, sidebar active glow, TOC/reading progress.
- Component pages automatically decorated with a pipeline header derived from `components:` front matter — zero per-page content edits.
- Both themes delivered together; dark mode leads.
- All motion respects `prefers-reduced-motion`.

**Non-Goals:**

- No framework migration, no content/IA changes, no console changes, no new runtime dependencies, no CI gate script changes (see proposal Non-goals).

## Decisions

### D1: Layered delivery — CSS first, Wrapping swizzles second, MDX/React kit third

Four layers, each independently shippable and visually coherent:

1. **Tokens + global surfaces (CSS only)** — custom properties in `custom.css`, restyle headings, code frames, admonitions, tables, navbar, sidebar, dark radial background.
2. **Wayfinding swizzles (Wrapping)** — `TOCItems` scroll-spy progress line; reading progress bar on doc pages.
3. **Decoration component kit** — `KindBadge`, `FeatureBadge`, `StatusDot` (shared MDX components) + swizzled `DocItem/Header` rendering the `PipelineHeader`.
4. **Hub pages** — docs home and `components.md` card gallery with kind badges.

Rationale: 80% of the perceived change comes from layer 1–2; layers 3–4 add identity. Layering bounds each PR's blast radius and lets any layer be reverted independently. Alternative rejected: one big-bang theme PR — harder to review, harder to bisect regressions.

### D2: PipelineHeader injects via swizzled `DocItem/Header`, not per-page MDX imports

A Wrapping swizzle of `DocItem/Header` reads `frontMatter.components` (already present on all 39 component pages, already CI-enforced) and renders the pipeline header above the title. Alternatives rejected:

- **Editing 39 MDX files to import an MDX component**: high churn, merge conflicts with ongoing doc edits, easy to forget on new component pages.
- **Remark plugin injecting JSX at build time**: more moving parts in the MDX pipeline; front matter is already available client-side, so the plugin buys nothing.

The swizzle is ~40 lines and degrades to a no-op on pages without `components:` front matter (127-ish other pages unaffected). Kind parsing maps `input/kafka` → badge label `KAFKA`, role segment `INPUT` in the pipeline strip.

### D3: Design tokens as CSS custom properties, `--af-*` namespace, extending Infima

`custom.css` already uses a small `--af-*` namespace (`--af-card-bg`, `--af-band-bg` at `custom.css:42-46`); the system extends it: `--af-gradient` (brand gradient `#2563eb → #38bdf8`, light) / (`#3b82f6 → #22d3ee`, dark), `--af-glow-*` (box-shadow glow levels), `--af-radius-*`, `--af-motion-*` (durations/easings), plus the existing `--af-accent` cyan `#22d3ee` for terminal motifs. Infima variables (`--ifm-*`) stay the single source of truth Docusaurus reads; `--af-*` composes on top. Rationale: no fork of Infima, theme switches keep working, and existing rules keep their meaning. Alternative rejected: Tailwind introduction — a new build dependency and idiom for no functional gain.

### D4: Terminalized code frames are CSS-only

The mac-dots title bar, terminal border, and dark-frame treatment on `.theme-code-block` are done with pseudo-elements and existing Docusaurus markup (the `title` meta string already renders a code block title). No CodeBlock swizzle. Prism theme pair (github / dracula) stays as-is this round — the frame carries the terminal identity; swapping syntax palettes is deferred to avoid a sitewide readability re-review.

### D5: Progress affordances use scroll listeners with reduced-motion/`@supports` fallbacks, not CSS scroll-timeline

`animation-timeline: scroll()` is still inconsistent across Safari/Firefox per the site's browserslist targets, so the TOC progress line and reading progress bar use a small rAF-throttled scroll listener (or IntersectionObserver for the TOC active state, which Docusaurus already does — we add only the progress fill). `prefers-reduced-motion` users get the state changes without animation. Docusaurus's existing scroll-spy logic is preserved — we restyle and add a fill, we do not replace the spy.

### D6: Glass and glow are budgeted

`backdrop-filter: blur` only on the navbar and (optionally) the desktop sidebar; glow shadows only on interactive/branded elements (cards, badges, active sidebar item, headings in the hero band, not body text). Body text stays solid color — gradient/glow text is restricted to display headings (`h1` doc titles, hub heroes) where contrast is dominated by size/weight. Every glow has an `@supports not (backdrop-filter: blur(1px))` solid fallback.

## Risks / Trade-offs

- [Docusaurus minor upgrades break swizzles] → Wrapping-only swizzles (never Ejecting), each swizzle < 100 lines delegating to the original component; swizzles isolated in `docs/src/theme/` so an upgrade diff is easy to rebase.
- [Glow/gradient text fails WCAG contrast in light mode] → gradient text only on large display headings; contrast checked at AA for body-adjacent elements in both themes; a per-theme screenshot checklist runs before each layer merges.
- [Motion fatigue / accessibility] → all animation gated on `prefers-reduced-motion` (pattern already established by the landing page's `prefersReducedMotion()`), micro-interaction durations ≤ 200ms, pipeline flow animation is a slow subtle dash-offset loop.
- [Light theme shipped as second-class] → both themes in the same layer PR; tasks include a light+dark verification pass per layer.
- [CI gate regressions from hub-page edits] → hub pages are existing registered pages; no new pages, no front-matter changes, no link changes — `docs:check` and lychee must pass unmodified as the layer's definition of done.
- [CSS-only code frame mis-renders on code blocks without titles] → frame styles target `.theme-code-block` uniformly; the dots row renders regardless of title presence (title bar shows dots + optional title), verified against untitled blocks in the layer checklist.

## Migration Plan

Rollout = the four layers of D1, in order, each a separate PR: tokens/global surfaces → swizzle affordances → decoration kit + pipeline header → hub pages. Each layer is independently revertible (CSS file(s) / theme swizzles / components + one swizzle / two pages). No build, data, or config migration; versioned docs snapshots (0.5.x) pick up the theme automatically since styling is site-wide. Rollback = git revert of the layer.

## Open Questions

- Docs-home hero motion — **Resolved during implementation (task 4.3)**: keep the current animated code window as the only hero motion; no flow-field background added.
- Announcement bar / version dropdown token adoption — **Resolved during implementation (layer 1 review)**: both stay stock; the announcement bar's solid brand-blue already reads consistently with the system.
- Dependency note: the DocItem/Content swizzle imports `useDoc` from `@docusaurus/plugin-content-docs/client`, which pnpm's strict layout cannot resolve from a transitive dependency. The package was added as an explicit dependency pinned to the already-lockfiled 3.9.2 (same version as the rest of the Docusaurus toolchain; no new code enters the bundle). This slightly amends the proposal's "no new npm dependencies" impact line — no runtime dependency is actually added.
