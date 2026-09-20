## Context

The docs visual system styles the page canvas in `docs/src/css/custom.css`:

- `custom.css:138` — `html { background-color: #ffffff }` (unconditional).
- `custom.css:175` — `[data-theme='dark'] html, [data-theme='dark'] body { background-color: #0b1120 }`.

Docusaurus sets `data-theme="dark"` on `<html>` itself, so the `[data-theme='dark'] html` descendant selector can never match. Separately, Infima's base stylesheet pins `html, body { height: 100% }`, which constrains `body`'s background box to one viewport height. Net effect in dark mode: the root canvas stays white and `body`'s dark paint stops after the first viewport, so every transparent region below the fold shows white under dark-theme text. Confirmed in a browser against the dev server (`data-theme=dark`, body box 720px vs document 3958px).

## Goals / Non-Goals

**Goals:**

- The dark canvas (`#0b1120` via `--ifm-background-color`) covers the root canvas at any scroll depth, on short pages, and during overscroll.
- Light mode pixel-identical to today.
- Surgical CSS change; no swizzle, content, or gate edits.

**Non-Goals:**

- No dark palette redesign; no changes to "always dark" landing sections.
- No visual-regression CI; verification is manual on the dev server plus existing gates.

## Decisions

### D1: Set the dark background on the root element, not on `body`

Apply `background-color: #0b1120;` inside the existing `[data-theme='dark']` token block (`custom.css:91`), which already matches `<html>`, and remove the dead `[data-theme='dark'] html` selector from `custom.css:175` (keep the `[data-theme='dark'] body` half).

- *Why hardcoded instead of `var(--ifm-background-color)`:* implementation revealed that Docusaurus re-declares that token on `html[data-theme='dark']` (specificity 0,1,1) with its own default `#1b1b1d`, which outranks the token value in the custom block (0,1,0) — a var() indirection silently resolves to the framework default. The `background-color` declaration itself still wins: the attribute selector (0,1,0) outranks Infima's `html { background-color: var(...) }` type selector (0,0,1).
- *Alternative rejected:* `min-height: 100%`-style layout fixes on `body` (or unpinning Infima's `height: 100%`) — larger blast radius, fights the framework base stylesheet, and still leaves the white canvas exposed on overscroll.

### D2: Keep the light-mode `html` rule untouched

`html { background-color: #ffffff }` stays; only the dark override is fixed. The top-anchored radial washes on `body` (`custom.css:180`) continue to paint as before in both themes.

### D3: Verification is manual and visual, against the running dev server

Checklist per the delta spec scenarios: docs page below the fold, landing transparent sections, overscroll, and a light-mode pass to confirm no drift. Then `pnpm docs:check` / `pnpm docs:build` untouched-and-green.

## Risks / Trade-offs

- [Dark `html` background changes the color visible in edge gutters on wide viewports] → The gutters already show `body`'s dark color within the first viewport; extending the same token to the canvas makes them consistent, not new.
- [Someone reintroduces a descendant `[data-theme]` selector in a future pass] → The delta spec scenario names the root-element requirement explicitly; the archived change documents why.
- [Dev-server HMR can mask or mimic the bug (styles load in varying order)] → Verify against a hard-reloaded page; the committed CSS is deterministic in production builds.

## Migration Plan

Single-commit CSS fix; no data, API, or config migration. Rollback is reverting the commit.

## Open Questions

(none)
