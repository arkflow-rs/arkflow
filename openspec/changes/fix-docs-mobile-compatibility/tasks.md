## 1. CSS: tables and pipeline header (P0)

- [x] 1.1 In `docs/src/css/custom.css`, add a `@media (max-width: 996px)` block turning `.markdown table` into a self-scrolling block (`display: block; width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch`), with a comment explaining the 996px choice and the stripe-width cosmetic cost
- [x] 1.2 In the same file, add a `@media (max-width: 640px)` block: `.af-pipeline__strip { flex-wrap: wrap; row-gap: 0.35rem; margin-left: 0; }` and `.af-pipeline__link { display: none; }`, with a comment stating the ~420px overflow math that motivates 640px
- [x] 1.3 Add `overflow-wrap: break-word` for long monospace identifiers in table cells below 996px (scoped to the same table media block, applied to `td`/`th`) and confirm it does not affect desktop

## 2. Mermaid and safe-area (P1/P2)

- [x] 2.1 In `docs/docusaurus.config.ts`, add `options: { flowchart: { useMaxWidth: false } }` to the `mermaid` themeConfig next to the existing `theme` block
- [x] 2.2 In `docs/src/css/custom.css`, give the mermaid container (`.docusaurus-mermaid-container`) a horizontal scroll (`overflow-x: auto`) and release Infima's `max-width: 100%` svg clamp (`max-width: none`) so natural-width diagrams scroll instead of shrinking or clipping
- [x] 2.3 Change `.af-reading-progress` `top` to `env(safe-area-inset-top, 0)` in `docs/src/css/custom.css`

## 3. Verification against the spec scenarios

- [x] 3.1 Run `pnpm docs:check` and `pnpm docs:build` from `docs/` — both must pass with no script or data-file changes
- [x] 3.2 Serve the built site and verify at 360×640 and 390×844, light and dark themes, that `document.documentElement.scrollWidth <= window.innerWidth` on: the docs home, a component reference page with a pipeline header, `reference/component-inventory.md`, `reference/compatibility.md`, `develop/kernel.md` (mermaid), `build/distributed-jobs.md`, and the landing page
- [x] 3.3 Verify the component-inventory table and a component config table scroll inside their own containers and all cells remain reachable by scrolling
- [x] 3.4 Verify `develop/kernel.md` mermaid renders legibly at 390px and scrolls horizontally; verify a desktop viewport (>996px) shows unchanged tables/pipeline headers and a scrollbar (not shrink) only for over-wide mermaid diagrams
- [x] 3.5 Confirm reduced-motion users see no new animation from the changed rules (wrap/hide are static) and the reading-progress bar still tracks scroll
