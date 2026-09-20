## ADDED Requirements

### Requirement: Translation freshness SHALL be reportable on demand
The documentation tooling SHALL provide a report command that compares every translated `zh-Hans` page with its English source by the last git commit that touched each file, listing stale-candidate translations (English source committed more recently than its translation) and coverage statistics (translated pages vs total translatable pages, excluding versioned trees and blog posts per policy). The report SHALL be informational only: it SHALL NOT fail regardless of staleness findings, preserving the drift policy that content staleness never fails a gate.

#### Scenario: A maintainer asks which translations lag
- **WHEN** the report command runs after an English page is edited without updating its translation
- **THEN** that page's translation is listed as a stale candidate together with coverage statistics, and the command exits 0

#### Scenario: Staleness never fails the build
- **WHEN** the repository contains any number of stale translations
- **THEN** the report command, `pnpm docs:check`, and CI all pass; only the report output names the stale candidates

#### Scenario: Never-translated trees are excluded
- **WHEN** the report computes coverage statistics
- **THEN** versioned documentation trees and blog posts are excluded from both the numerator and the denominator
