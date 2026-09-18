## Why

The docs visual system merged in #1234 is not usable on phone viewports (360–414px): on component reference pages the pipeline role strip overflows its container and the page gains horizontal scroll (`docs/src/css/custom.css:685-689` — `.af-pipeline__strip` is a non-wrapping `inline-flex` whose min-content width ≈ 360px exceeds the ≈ 314px available on a 375px screen), and every markdown table in the docs overflows the viewport because there is no horizontal scroll container (`docs/src/css/custom.css:288-291` — `.markdown table { display: table; width: 100% }`; Docusaurus 3.9.2 provides none). The worst affected page is the generated component inventory (`docs/docs/reference/component-inventory.md:9-52`, 40+ rows × 4 columns). Wide mermaid diagrams degrade to unreadable text instead of scrolling (`docs/docs/develop/kernel.md:19-26` combined with the mermaid config in `docs/docusaurus.config.ts:198-203`, which leaves `useMaxWidth` at its scaling default), and the reading-progress bar ignores iOS safe-area insets (`docs/src/css/custom.css:589-595`). The `docs-visual-system` spec has no mobile scenarios, so none of this was caught.

## What Changes

- Add a small-viewport (≤ ~640px) CSS layer in `docs/src/css/custom.css` that:
  - lets the pipeline role strip wrap and hides the dashed connectors on narrow screens, so the INPUT → BUFFER → PROCESSOR → OUTPUT chips stack instead of overflowing;
  - turns markdown tables into self-scrolling blocks (`display: block; overflow-x: auto` bounded by `max-width: 100%`) so wide tables scroll inside their own container instead of stretching the page;
  - adds `overflow-wrap` for long monospace identifiers inside table cells.
- Make wide mermaid diagrams scroll at natural size instead of shrinking: set `flowchart: { useMaxWidth: false }` via the preset's mermaid `options` and give `.mermaid` a horizontal scroll container (same UX as the landing-page architecture diagram).
- Respect `env(safe-area-inset-top)` on the fixed reading-progress bar.
- Amend the `docs-visual-system` spec with an explicit mobile-viewport requirement and scenarios (375px/360px, both themes, no page-level horizontal scroll), closing the gap that let this ship.

## Capabilities

### New Capabilities

- (none)

### Modified Capabilities

- `docs-visual-system`: add a requirement that the visual system remains usable at mobile viewports — pipeline header wraps without overflow, wide tables scroll within their own container, wide mermaid diagrams scroll at natural size, and no docs page produces page-level horizontal scroll at 360–414px in either theme. No existing requirement changes.

## Impact

- `docs/src/css/custom.css` — the bulk of the fix (one small-viewport media-query block, table base tweak, safe-area inset, mermaid scroll container).
- `docs/docusaurus.config.ts` — mermaid `options.flowchart.useMaxWidth` only.
- No Rust code, no swizzled theme components, no CI gate scripts: `pnpm docs:check`, example validation, inventory generation, and link checks are untouched and must keep passing unchanged (per the existing `docs-visual-system` CI-gate requirement).
- Every docs page's rendering below ~640px viewport width changes (tables and pipeline headers most visibly); desktop rendering above the breakpoint is intentionally pixel-identical except mermaid blocks wider than the content column, which gain a scrollbar instead of shrinking.
