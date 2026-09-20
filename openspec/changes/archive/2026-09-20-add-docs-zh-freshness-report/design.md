## Context

All 128 translatable pages have zh-Hans counterparts, but English is canonical and translations may lag (`docs/DOCUMENTATION.md:101`). Nothing currently tells a contributor or maintainer WHICH translations lag. Git already records the last commit per file, which is a sufficient staleness proxy: if an English page's last content commit is newer than its translation's, the translation may be stale.

## Goals / Non-Goals

- **Goal**: one-command freshness report (stale candidates + coverage stats) that never fails.
- **Non-Goal**: gate enforcement, CI wiring, content fixes, translation tooling beyond reporting.

## Decisions

### D1: Git-timestamp comparison, not content diff
`git log -1 --format=%ct -- <path>` per file; a translation is *stale-candidate* when its English source's last commit timestamp is strictly newer. 
*Alternative*: structural or semantic diffing — rejected: expensive, noisy, and the report only needs to rank attention, not prove divergence.

### D2: Report-only, exit 0
The script prints a markdown-readable table and exits 0 regardless of findings; staleness must not fail gates per `documentation-i18n`. 
*Alternative*: `--fail-on-stale` flag — deferred until someone wires it into CI.

### D3: Scope mirrors the translation policy
Only pages under `docs/docs/` with a mirrored counterpart are compared; `versioned_docs/` and blog are excluded (never translated). Sidebar/chrome JSONs are out of scope (they track config, not content drift).

### D4: Noise guard for the freshness signal
A translation counts as stale only if the English page changed AFTER the zh page's last commit — same-day commits (±0 seconds ties) count as fresh, and a `--verbose` flag lists fresh pairs too. No grace-window heuristics (keep the rule predictable).

## Risks / Trade-offs

- [Git timestamps reflect commits, not content relevance (trivial edits flag stale)] → accepted: the report is a triage list, not a verdict; D2 keeps it non-blocking.
- [Reports differ between shallow CI clones] → documented as a local-development tool; CI wiring is a non-goal.

## Migration Plan

Purely additive tooling; rollback = remove script + package.json entry + doc note.

## Open Questions

(none)
