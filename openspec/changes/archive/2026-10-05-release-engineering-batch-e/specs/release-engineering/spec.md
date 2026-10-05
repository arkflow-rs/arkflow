# Delta: release-engineering

## ADDED Requirements

### Requirement: CI SHALL enforce formatting and lint gates that actually run

The repository's Rust CI SHALL execute `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` as blocking steps on every push and pull request. Installing lint components without executing them SHALL NOT count as a gate.

#### Scenario: A pull request introduces a formatting drift

- **WHEN** a pull request contains code that `cargo fmt` would rewrite
- **THEN** the CI formatting step fails and the pull request is blocked until the drift is removed

#### Scenario: A pull request introduces a clippy warning

- **WHEN** a pull request contains code that triggers any clippy warning
- **THEN** the CI lint step fails (warnings are deny-by-default) and the pull request is blocked until the warning is fixed

### Requirement: Pushing a version tag SHALL produce multi-platform binary artifacts on a GitHub Release

Pushing a git tag matching `v*` SHALL trigger a release workflow that builds release binaries for Linux amd64, Linux arm64, macOS amd64, and macOS arm64, packages each as an archive containing the binary plus LICENSE and README, and attaches all archives to a GitHub Release for that tag. The workflow SHALL verify the tag name matches the workspace version before building.

#### Scenario: A version tag is pushed

- **WHEN** a maintainer pushes a tag `v1.0.0` while the workspace version is `1.0.0`
- **THEN** the workflow builds four platform archives named with the tag and target triple and attaches them to the `v1.0.0` GitHub Release

#### Scenario: A tag does not match the workspace version

- **WHEN** a tag `v1.1.0` is pushed while the workspace version is still `1.0.0`
- **THEN** the workflow fails before building with an actionable message telling the maintainer to bump the version first

#### Scenario: The release workflow is exercised without a tag

- **WHEN** the workflow is triggered manually via `workflow_dispatch`
- **THEN** the build matrix and packaging steps run to completion without creating or modifying any GitHub Release

### Requirement: Crate packaging SHALL be validated in CI before any publish

The release workflow SHALL include a job that runs `cargo publish --dry-run` for each publishable crate in dependency order (arkflow-core, arkflow-plugin, arkflow, arkflow-server). The job SHALL NOT perform a real publish; actual publishing remains a maintainer action.

#### Scenario: A crate's packaging metadata breaks

- **WHEN** a change introduces invalid packaging metadata (for example a missing license field or an unresolvable dependency version)
- **THEN** the dry-run job fails and the release is blocked

#### Scenario: Publish order is wrong

- **WHEN** the dry-run job executes with a crate listed before its dependency crate
- **THEN** the job configuration is defined so the workspace's publishable crates always run in dependency order, preventing false failures from ordering

### Requirement: The repository SHALL maintain a user-facing changelog

The repository root SHALL contain a CHANGELOG following the Keep a Changelog format. Until a version is released, entries accumulate under an `Unreleased` heading. When a version tag is created, the maintainer SHALL rename the `Unreleased` section to that version with a date. Changelog entries SHALL cover user-perceivable changes (new components, new configuration, behavior changes, notable fixes) and MAY summarize internal refactoring without listing every internal change.

#### Scenario: A user-visible feature lands without a changelog entry

- **WHEN** a pull request adds a new component or changes documented behavior but does not update CHANGELOG.md
- **THEN** reviewers treat the changelog update as part of the definition of done for that pull request

#### Scenario: A version is released

- **WHEN** the maintainer tags a release
- **THEN** the accumulated `Unreleased` section is renamed to the released version with the release date, and a fresh empty `Unreleased` section is in place

### Requirement: The repository SHALL publish a security policy

The repository root SHALL contain a SECURITY.md stating how to report vulnerabilities (private channels first, with a maintainer contact as fallback), which versions receive security fixes, and the vulnerability handling scope.

#### Scenario: A researcher finds a vulnerability

- **WHEN** someone discovers a security issue in a released artifact
- **THEN** SECURITY.md directs them to a private reporting channel, and the policy commits maintainers to acknowledge and fix according to the stated supported-versions table
