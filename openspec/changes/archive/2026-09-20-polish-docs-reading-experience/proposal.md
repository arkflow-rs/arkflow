## Why

The engine×premium visual system (#1234) and the mobile-compatibility fix (#1235) cover headings, code frames, tables, admonitions, wayfinding progress, and small viewports — but several everyday reading surfaces are still stock Docusaurus: the prev/next pagination is an unstyled bordered box, blockquotes render with the bare Infima border (the intro page and the codec reference pages use them for note-style callouts), code blocks never say which language they contain unless the author wrote a title, the copy button floats detached from the terminal-frame metaphor, breadcrumbs and heading hash-links are unstyled, and long reference pages have no back-to-top affordance. These are the remaining gap between "restyled site" and "finished reading experience", and they all sit on tokens that already exist.

## What Changes

- Restyle the doc-page pagination (`.pagination-nav`) as brand cards within the existing `--af-*` token system: card surface, sublabel/label typography, hover lift with glow and border tint (mirroring `.af-cards`).
- Add a back-to-top affordance: a small component inside the existing `DocRoot` wrapping swizzle, rendered on docs pages only — appears after scrolling past roughly a viewport, smooth-scrolls up, and jumps instantly under `prefers-reduced-motion`.
- Give blockquotes a quiet brand treatment (3px gradient bar echoing the admonition accent plus a barely-tinted band) so the note-style quotes on the intro and codec reference pages stop reading as unfinished markup.
- Finish the code-block terminal bar: untitled blocks get a monospace language micro-label on the right of the title bar, and the copy button is toned into the bar (muted at rest, brand on hover; stock copied-state mechanics preserved).
- De-emphasize breadcrumbs, reveal heading hash-links on hover (always visible on keyboard focus), and add hover underlines with offset to markdown links for clearer clickability.
- Run a refinement pass over the whole reading surface so the new elements don't sit on a plain base: self-hosted Inter variable font with antialiased rendering, a layered shadow token system (`--af-shadow-sm/card`, `--af-inset-highlight`), an ease-out motion curve, typography rhythm (body size, heading margins/tracking, list spacing), softened table zebra with a hairline header rule, a quiet deep-navy announcement bar, a tinted sidebar active pill, circular arrow chips on the pagination cards, focus-visible ring, and a brand `::selection`.
- Amend the `docs-visual-system` spec with a finishing-affordances requirement and scenarios.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `docs-visual-system`: add a requirement that reading surfaces carry finishing wayfinding and interaction affordances — pagination cards, back-to-top with reduced-motion compliance, styled blockquotes, terminal-bar language chip and copy button, subdued breadcrumbs/hash-links. No existing requirement changes.

## Impact

- `docs/src/css/custom.css` — the bulk of the change (pagination, blockquote, code-bar chip/copy button, breadcrumbs, hash-links, links, back-to-top styles, plus the token/typography/depth refinement layer).
- `docs/src/theme/DocRoot/index.tsx` — one small additional component beside the reading-progress bar (still a Wrapping swizzle, still delegating to stock `DocRoot`).
- `docs/src/theme/CodeBlock/Container/index.tsx` — a tiny Wrapping swizzle forwarding the fence language as `data-language` and mounting the terminal-bar micro-label.
- `docs/docusaurus.config.ts` — announcement bar base colors only (`#0b1120` / `#cbd5e1`; the CSS layers a gradient above them).
- `docs/package.json` — one new dependency, `@fontsource-variable/inter` (self-hosted font, bundled at build time, no external requests).
- No content, markdown, or CI-gate changes: `pnpm docs:check`, example validation, inventory generation, and link checks are untouched and must keep passing unchanged.
- Desktop rendering changes visually (pagination, blockquotes, code bars, back-to-top, typography, depth are new treatments); layout metrics stay token-driven, and the mobile-compatibility behavior from #1235 must not regress.
