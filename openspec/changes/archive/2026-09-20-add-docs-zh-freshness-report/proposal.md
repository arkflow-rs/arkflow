## Why

The zh-Hans rollout is complete: all 128 non-versioned, non-blog pages under `docs/docs/` now have Simplified Chinese counterparts (per `openspec/specs/documentation-i18n/spec.md`, requirements "The components reference SHALL be translated" through "The operations and reference domain SHALL be translated"). English stays canonical (`docs/DOCUMENTATION.md:101`), so translations will inevitably drift as English pages evolve. The original i18n proposal explicitly deferred a staleness report until coverage passed ~50 pages (`openspec/changes/archive/2026-09-20-add-docs-i18n-zh/proposal.md`, Non-goals) — that threshold is now far exceeded, and with 128 translated pages review discipline alone can no longer track drift.

## What Changes

- **New `docs/scripts/i18n-freshness-report.mjs`**: compares each translated page against its English source by last git commit that touched the file, and reports (1) stale translations — zh page last touched before its English counterpart was meaningfully changed, (2) coverage — translated/total page counts, (3) never-translated trees (versioned docs, blog) excluded per policy.
- **Report-only, never a gate failure**: staleness SHALL NOT fail any check (per the documented drift policy); the script exits 0 always.
- **New `docs:i18n-report` pnpm script** and a short usage note in `docs/DOCUMENTATION.md`'s i18n section so contributors can check freshness before editing a translation.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `documentation-i18n`: adds a requirement that translation freshness SHALL be reportable on demand (stale-translation detection and coverage stats), while keeping the existing rule that staleness never fails a gate.

## Impact

- **`docs/scripts/i18n-freshness-report.mjs`** (new), **`docs/package.json`** (one script entry), **`docs/DOCUMENTATION.md`** (i18n section note).
- No changes to `docs:check`, the Rust validators, CI, or any page content.
- Tooling only — no runtime/website behavior change.

## Non-goals

- Failing CI on staleness (contradicts the documented drift policy).
- Machine-translation suggestions, Crowdin integration, or per-paragraph diff rendering.
- Translating versioned docs or blog posts.

## Task Batches

1) report script + pnpm entry, 2) documentation note, 3) validation.
