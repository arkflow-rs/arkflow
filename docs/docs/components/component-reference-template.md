---
sidebar_label: Component reference template
sidebar_position: 0
---

# Component reference template

Every component page follows the same order so readers can move between
inputs, processors, buffers, codecs, and outputs without relearning the page.

## Required page shape

1. A one-line scenario statement and the component's `## Status` (`Stable`,
   `Beta`, or `Experimental`).
2. `## When to use`, including a short note about when to choose another
   component.
3. `## Common fields`, followed by `## Full reference`.
4. A fenced, generated table with the columns `Field`, `Type`, `Required`,
   `Default`, and `common?`. The generator owns everything between the
   `<!-- BEGIN AUTO: ... -->` and `<!-- END AUTO -->` markers.
5. `## Examples` with at least two distinct scenarios.
6. `## Output schema` for inputs/codecs, or `## Input schema` for outputs
   where applicable.
7. `## Error handling`, `## Metrics`, and `## See also`.

Run `pnpm components:generate` from `docs/` after changing component metadata.
The generator obtains field names, types, requiredness, and defaults from the
same `arkflow components show ... --format json` interface used by the CLI.
