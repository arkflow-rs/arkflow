## ADDED Requirements

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
