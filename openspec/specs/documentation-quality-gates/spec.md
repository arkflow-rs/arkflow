# Purpose

Define local and CI quality gates that prevent broken ArkFlow documentation from being published.

## Requirements

### Requirement: Documentation quality checks SHALL run locally and in CI
The repository SHALL expose one documented validation command that checks page metadata/structure, sidebar reachability, internal links and anchors, reference coverage, and configured examples, and CI SHALL run the same command.

#### Scenario: A pull request has a broken internal link
- **WHEN** the documentation validation command runs
- **THEN** it fails with the source page and unresolved target so the pull request cannot pass the documentation gate.

#### Scenario: Documentation CI and local validation are compared
- **WHEN** CI executes the documentation check
- **THEN** it uses the repository's documented command and does not maintain a divergent duplicate implementation of the checks.

### Requirement: Documentation builds SHALL be release-blocking after baseline cleanup
The production site build SHALL be a required validation step, and known exceptions SHALL be explicit, owned, and reviewable rather than hidden by broad warning settings.

#### Scenario: A page references a missing anchor
- **WHEN** the production documentation build runs
- **THEN** the build or validation gate fails or reports an explicit tracked exception with the affected route and owner.

#### Scenario: A code/configuration example is invalid
- **WHEN** an example is marked as executable or validation-targeted
- **THEN** the checker runs the appropriate syntax/config validation and fails with actionable file and line information.
