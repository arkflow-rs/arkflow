## Context

The visual system is centralized in `docs/src/css/custom.css` (~845 lines) with `--af-*` tokens composing on top of Infima, plus three wrapping swizzles (`DocRoot`, `TOCItems`, `DocItem/Content`) and a custom `NotFound`. The surfaces this change touches are the ones #1234 deliberately left stock: `.pagination-nav`, `blockquote`, the code-block copy button and untitled bars, `.theme-doc-breadcrumbs`, `.hash-link`, and markdown links. The `DocRoot` swizzle already hosts a scroll-listening component (`ReadingProgress`) whose pattern (rAF-throttled passive listener, CSS custom property as output, reduced-motion safety net) the back-to-top button can follow. The `docs-visual-system` spec constrains this work: tokens must degrade to stock Infima, swizzles stay Wrapping and small, reduced-motion is a sitewide requirement, and CI gates must pass untouched.

Constraints: no new npm dependencies; no content/markdown changes; `pnpm docs:check` and the Rust docs-validation tests must pass with gates' scripts and data unchanged; the #1235 mobile behavior (no page-level horizontal scroll at 360px) must not regress.

## Goals / Non-Goals

**Goals:**

- Every frequently-seen reading surface carries the visual system: pagination, blockquotes, code terminal bar (language + copy), breadcrumbs, hash-links, links.
- Long reference pages get a back-to-top affordance consistent with the existing wayfinding pieces.
- All new styling is token-driven, degrades to stock Infima if tokens are removed, and works in both themes.
- Spec amendment so the finishing affordances become enforced requirements, not one-off styling.

**Non-Goals:**

- No landing page, navbar, footer, sidebar, search modal, or 404 changes.
- No new npm dependencies (the back-to-top arrow is an inline SVG; no icon library).
- No Tabs/`<details>` styling — the docs currently use neither; dead CSS is not added.
- No content rewrites and no CI gate script changes.
- No Ejecting swizzles (spec requirement): only small Wrapping swizzles.

## Decisions

### D1: Pagination becomes brand cards (CSS only)

Restyle `.pagination-nav__link` with the `.af-cards` surface language: `--af-card-bg`/`--af-card-border`, `--af-radius-card`, hover `translateY(-3px)` + `--af-glow-md` + primary border tint; restyle `.pagination-nav__sublabel` as an uppercase micro-label and `.pagination-nav__label` as semibold; add a static directional arrow via `::after` (hidden below ~640px where cards stack vertically). Alternatives: (a) leave stock — rejected, it is the most-seen unstyled surface on every doc page; (b) a custom pagination swizzle — rejected, stock DOM (`--previous`/`--next` items, sublabel/label divs) is fully sufficient for CSS-only treatment.

### D2: Back-to-top lives in the DocRoot swizzle, ReadingProgress-style

A `BackToTop` component added beside `ReadingProgress` in `docs/src/theme/DocRoot/index.tsx`: rAF-throttled passive scroll listener toggling `data-visible` once `scrollY` passes ~one viewport; click calls `window.scrollTo({top: 0, behavior: reduced-motion ? 'auto' : 'smooth'})`. Rendering inside `DocRoot` scopes it to docs pages automatically (landing page, blog, and 404 keep their own layouts). Styling: fixed bottom-right circle (~2.75rem) honoring `env(safe-area-inset-bottom)`, card surface + brand ring, glow on hover, inline SVG chevron, `aria-label="Back to top"`, visible only when scrolled (also hidden from the tab order via `tabindex=-1` when hidden). Alternatives: (a) a new global `Root` swizzle to cover blog/landing too — rejected, those pages are short or have their own scroll UI and the spec says DocRoot wrapping only; (b) CSS-only anchor link — rejected, unreliable scroll restore and no progressive reveal. Z-index stays below Infima's fixed surfaces (navbar/drawer) so drawers always cover it; exact value pinned against built CSS during implementation.

### D3: Blockquotes get the quiet admonition echo

`blockquote` becomes a rounded band with a 3px gradient bar (`::before`, echoing `.admonition::before`) and a barely-tinted background (`color-mix` on `--ifm-color-emphasis-100`), text at `--ifm-color-emphasis-800`. Deliberately quieter than admonitions — blockquotes in this docs tree carry note-style callouts (the intro page, the codec reference "Notes") whose content is incidental, so the treatment must read as texture, not as a callout of its own. Alternatives: (a) stock border-left — rejected, that is the "unfinished" look being fixed; (b) admonition-strength tinting — rejected, note quotes should not compete with real admonitions.

### D4: Language chip binds to a stock DOM hook; swizzle only if none exists

Goal: untitled code blocks show their language in the terminal bar's right side, so the chip must not collide with the stock copy button. The chip is CSS (`::after` with `content: attr(...)`) bound to whatever the MDX pipeline exposes on the `.theme-code-block` container — a `data-language` attribute or a `language-<x>` class. Implementation verifies the built HTML first (`pnpm docs:build` + inspect `docs/build`): if a data attribute exists, bind directly; if only a `language-<x>` class exists, add a minimal Wrapping swizzle of `CodeBlock/Container` that forwards `data-language` (still small enough to rebase across upgrades, per spec); if neither hook is stable, the chip is dropped and the task closed as not-feasible-without-noise. The copy button is toned into the bar with color/position only — the stock copied-state icon swap is preserved untouched.

### D5: Breadcrumbs, hash-links, and links get micro-treatments

Breadcrumbs: slightly smaller, muted (`--ifm-color-emphasis-600`), primary on hover. Hash-links: `opacity: 0` revealed on heading hover, always visible on `:focus-visible` (keyboard users keep the affordance; reduced-motion net covers the transition). Markdown links inside `article .markdown`: no underline at rest (color affordance preserved), `text-decoration: underline` with `text-underline-offset: 3px` on hover. All three are one-rule changes; none alter layout metrics.

### D7: Premium refinement layer keeps the base style

After the affordances shipped, a refinement pass raised the perceived quality without changing the design language:

- **Type**: self-hosted Inter Variable (`@fontsource-variable/inter`, bundled by webpack — no external requests) as `--ifm-font-family-base`, antialiased rendering + `optimizeLegibility`, body at 1.035rem with relaxed list spacing, h2/h3 margin rhythm (3rem/2.1rem) and tightened tracking. Alternative considered: Google Fonts link — rejected, external request and offline-unfriendly.
- **Depth**: layered shadow tokens `--af-shadow-sm` (hairline) and `--af-shadow-card` (ambient + key), applied to code blocks, cards, pagination, and back-to-top; an inset top highlight on the code terminal bar; the ease curve upgraded to `cubic-bezier(0.22, 1, 0.36, 1)`.
- **Chrome**: announcement bar base moved to deep navy `#0b1120` (config) with a faint gradient sheen painted via CSS above the inline color; navbar glass upgraded to blur(12px) saturate(160%) + hairline border; sidebar active item gets a 6% brand pill behind the existing gradient bar; `::selection` and `:focus-visible` use brand tints.
- **Pagination arrows** become 2rem circular chips (border + brand glyph) that nudge 3px outward on hover; the stock `«`/`»` label glyphs are suppressed since the chips carry the direction.
- All of it stays token-driven; stripping `--af-*` tokens degrades exactly like the shipped system (verified: border drops to 0px, Infima emphasis tokens keep text/band readable).

### D8: Spec amendment as one ADDED requirement

The finishing affordances are one requirement of `docs-visual-system` ("Reading surfaces SHALL carry finishing wayfinding and interaction affordances") with scenarios for pagination cards, back-to-top (appearance threshold, reduced-motion instant jump), blockquote treatment, language chip + copy button in the terminal bar, and unchanged CI gates. Amending the existing capability (like #1235 did) instead of a new capability: these are the same reading surfaces the capability already owns.

## Risks / Trade-offs

- [Back-to-top overlaps footer content or the version dropdown area on short pages] → It only appears after ~one viewport of scroll and sits above the footer's end; verified on short and long pages in acceptance.
- [Language chip collides with long titles in the terminal bar] → Title already truncates with ellipsis and reserves right padding; chip is clipped behind the copy button zone only if both exist — chip renders on untitled blocks only, so they never coexist with a title (copy button + chip coexistence checked in acceptance).
- [Blockquote band too loud where notes stack] → Band tint is near-invisible by design (D3); visually checked on a codec reference page in both themes.
- [Mobile regression] → All additions are block-level or fixed-position elements already covered by the #1235 audit; scroll-width check re-run at 360px in acceptance.
- [Swizzle growth risk] → `DocRoot` gains one ~40-line component; `CodeBlock/Container` swizzle only if D4 requires it, and it forwards a single attribute.

## Migration Plan

Single docs-site commit; no build flags, no data migration. Deploy is the normal docs publish. Rollback is reverting the commit. CI gates are untouched and must pass unchanged (`pnpm docs:check`, `pnpm docs:build`, `pnpm typecheck`, workspace docs tests) before merge.

## Open Questions

- None at proposal time. The only fork (language-chip DOM hook) has a decided verification order in D4, including the "drop the chip" fallback.
