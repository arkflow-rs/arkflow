# Purpose

Define versioning, release, archival, compatibility, and audit rules for ArkFlow documentation.

## Requirements

### Requirement: Documentation SHALL define a version and release lifecycle
The project SHALL document which version is current, which versions remain supported, when snapshots are created, how incompatible changes are labeled, and when old versions are archived.

#### Scenario: A release is prepared
- **WHEN** maintainers prepare a release
- **THEN** the release checklist verifies current docs, version metadata, links, examples, generated references, and the version dropdown before publication.

#### Scenario: A feature is unavailable in an older version
- **WHEN** a reader views a versioned page for a release that does not support the feature
- **THEN** the page clearly states the compatibility boundary and directs the reader to the supported version or alternative.

### Requirement: Documentation releases SHALL be reproducible and auditable
The repository SHALL record the commands and inputs used to generate/validate the published docs and SHALL make the resulting version/build status visible in CI artifacts or release notes.

#### Scenario: A published site is investigated
- **WHEN** a maintainer needs to reproduce a documentation build
- **THEN** the selected version, dependency lockfile, validation command, and build result are available from repository or CI records.

### Requirement: Product versioning policy and upgrade guidance SHALL be documented

The documentation site SHALL include a versioning page (in both English and the zh-Hans tree) defining the semantic versioning commitment, what the v1.0 public surface freeze covers, and step-by-step upgrade guidance between versions. The page SHALL complement, not duplicate, the documentation snapshot compatibility page.

#### Scenario: A user upgrades a deployment

- **WHEN** a user reads the versioning page before upgrading an ArkFlow deployment
- **THEN** they find the supported upgrade path, configuration or API breaking changes to review, and pointers to the changelog

#### Scenario: The zh-Hans tree is checked

- **WHEN** the English versioning page is added or changed
- **THEN** the zh-Hans mirror page exists with equivalent content, and `pnpm docs:check` passes
