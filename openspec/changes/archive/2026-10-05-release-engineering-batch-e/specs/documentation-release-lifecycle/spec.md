# Delta: documentation-release-lifecycle

## ADDED Requirements

### Requirement: Product versioning policy and upgrade guidance SHALL be documented

The documentation site SHALL include a versioning page (in both English and the zh-Hans tree) defining the semantic versioning commitment, what the v1.0 public surface freeze covers, and step-by-step upgrade guidance between versions. The page SHALL complement, not duplicate, the documentation snapshot compatibility page.

#### Scenario: A user upgrades a deployment

- **WHEN** a user reads the versioning page before upgrading an ArkFlow deployment
- **THEN** they find the supported upgrade path, configuration or API breaking changes to review, and pointers to the changelog

#### Scenario: The zh-Hans tree is checked

- **WHEN** the English versioning page is added or changed
- **THEN** the zh-Hans mirror page exists with equivalent content, and `pnpm docs:check` passes
