## 1. Pagination and blockquote (P0)

- [x] 1.1 In `docs/src/css/custom.css`, restyle `.pagination-nav__link` as a brand card (`--af-card-bg`, `--af-card-border`, `--af-radius-card`, hover lift + `--af-glow-md` + border tint mirroring `.af-cards a.card`), with `.pagination-nav__sublabel` as an uppercase micro-label and `.pagination-nav__label` semibold, plus a directional `::after` arrow hidden below 640px (the stock `«`/`»` label glyphs are also suppressed — doubling with the arrows read as clutter)
- [x] 1.2 In the same file, restyle `blockquote` as a rounded band with a 3px gradient `::before` bar and a barely-tinted `color-mix` background, text at emphasis-800, in both themes (quiet treatment per design D3; real usage is the intro page and codec reference notes)

## 2. Back-to-top (P0)

- [x] 2.1 Extend `docs/src/theme/DocRoot/index.tsx` with a `BackToTop` component beside `ReadingProgress`: rAF-throttled passive scroll listener, `data-visible` past ~one viewport, click scrolls to top with `behavior: 'auto'` under `prefers-reduced-motion` and `'smooth'` otherwise, `aria-label="Back to top"`, hidden from tab order while not visible
- [x] 2.2 Add `.af-back-to-top` styles: fixed bottom-right (~2.75rem circle, `env(safe-area-inset-bottom)` offset), card surface + brand ring, glow on hover, inline SVG chevron, opacity/transform reveal gated on `data-visible`, z-index below Infima fixed surfaces (pinned against built CSS: `--ifm-z-index-fixed` is 200, overlay 400, button 170)

## 3. Code-block terminal bar finishing (P1)

- [x] 3.1 Build the site and inspect the emitted `.theme-code-block` DOM to pick the language-chip hook (`data-language` attribute vs `language-<x>` class vs none — design D4 decision path). Finding: the container only carries a `language-<x>` class (from `ensureLanguageClassName` in theme-common); no data attribute exists
- [x] 3.2 Implement the language micro-label for untitled blocks in the terminal bar's right side (CSS `attr()` binding, or a minimal `CodeBlock/Container` Wrapping swizzle forwarding `data-language` if only a class exists); drop the chip if no stable hook — implemented as the swizzle forwarding `data-language` plus a mounted `.af-code-lang` span (no context hook: Container also renders for JSX `<pre>` content outside the provider, so the language is parsed from `className`; the chip is suppressed when a title child exists). Note: no titled fences exist in the docs tree today, so every block currently shows the chip
- [x] 3.3 Tone the copy button into the terminal bar: muted icon at rest, brand color on hover/focus, position centered in the 2.1rem bar, stock copied-state mechanics untouched (button group lifted via `top: -1.05rem; transform: translateY(-50%)`; faint 0.45 opacity at rest so touch users can find it)

## 4. Breadcrumbs, hash-links, links (P2)

- [x] 4.1 Style `.theme-doc-breadcrumbs` (smaller, muted, primary on hover) and `.hash-link` (hidden at rest, revealed on heading hover and always on `:focus-visible`)
- [x] 4.2 Add hover underline with `text-underline-offset: 3px` to `article .markdown a` links

## 6. Premium refinement pass (P1)

- [x] 6.1 Add `@fontsource-variable/inter` and wire it as `--ifm-font-family-base` with antialiased rendering, `optimizeLegibility`, `::selection` brand tints, and a `:focus-visible` ring
- [x] 6.2 Add layered shadow tokens (`--af-shadow-sm`, `--af-shadow-card`, `--af-inset-highlight`) in both themes and apply them to code blocks, hub cards, pagination cards, and back-to-top; upgrade `--af-motion-ease` to an ease-out curve
- [x] 6.3 Typography rhythm: `.markdown` body 1.035rem, h2/h3 margins and tracking, list spacing; soften table zebra to 22% and add the hairline header rule
- [x] 6.4 Chrome: announcement bar to deep navy (config base `#0b1120`/`#cbd5e1` + CSS gradient sheen), navbar blur(12px) saturate(160%) + hairline border, sidebar active pill
- [x] 6.5 Pagination arrows as circular chips with hover nudge; suppress stock `«`/`»` label glyphs
- [x] 6.6 Rebuild and re-verify: typecheck, docs:check, docs:build, desktop/mobile screenshots in both themes

## 5. Verification against the spec scenarios

- [x] 5.1 Run `pnpm docs:check`, `pnpm typecheck`, and `pnpm docs:build` from `docs/` — all pass with no script or data-file changes
- [x] 5.2 Serve the built site and verify on desktop and at 375px in both themes: pagination cards render and hover correctly (a page with both prev/next), blockquote band on a codec reference page, back-to-top appears after a viewport of scroll and reaches the top, language chip shows on an untitled block and the copy button sits in the terminal bar (dark-theme verification done via computed styles — body/card/button all resolve dark tokens; the embedded browser's screenshot compositor blends stale frames, so computed values were used as ground truth. Click handler verified to invoke `scrollTo({top: 0, behavior: 'smooth'})` with correct args and the page reaching scrollY 0; the environment does not deliver `scroll` events for programmatic scrolls, so the reveal/hide flip was confirmed through the event-driven path the reading-progress bar already uses, which updated correctly)
- [x] 5.3 Verify reduced-motion: back-to-top jumps instantly (no smooth scroll), hash-link reveal applies without transition delay (both covered by the sitewide `prefers-reduced-motion` net plus the explicit `matchMedia` branch in the click handler; hash-link reveal is hover-only media so no delay exists)
- [x] 5.4 Verify no mobile regression: `document.documentElement.scrollWidth <= window.innerWidth` at 360×640 on a component page, `reference/component-inventory.md`, and a SQL reference page, in both themes (all six combinations: 350 <= 350, no page-level scroll; only `CODE.codeBlockLines` extends past the viewport inside its own scroll container, the intended pattern)
- [x] 5.5 Confirm `--af-*` degradation: removing a new rule's tokens leaves stock Infima rendering (spot-check pagination and blockquote with tokens stripped). Finding: stripping tokens inline degrades pagination/blockquote identically to the shipped `.af-cards` rules (card border drops to 0px; blockquote band survives on Infima emphasis tokens, only the gradient bar disappears) — consistent with the visual system's established degradation behavior; layout and readability stay intact
