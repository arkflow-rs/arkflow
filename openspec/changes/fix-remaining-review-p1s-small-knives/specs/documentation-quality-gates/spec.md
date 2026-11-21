## MODIFIED Requirements

### Requirement: Documentation quality checks SHALL run locally and in CI
The repository SHALL expose one documented validation command that checks page metadata/structure, sidebar reachability, internal links and anchors, reference coverage, and configured examples, and CI SHALL run the same command. The CI job's trigger path filter SHALL cover every directory that carries checked content, including `examples/**`, so changes to example configuration files cannot bypass the documentation gate.

#### Scenario: A pull request has a broken internal link
- **WHEN** the documentation validation command runs
- **THEN** it fails with the source page and unresolved target so the pull request cannot pass the documentation gate.

#### Scenario: Documentation CI and local validation are compared
- **WHEN** CI executes the documentation check
- **THEN** it uses the repository's documented command and does not maintain a divergent duplicate implementation of the checks.

#### Scenario: A pull request only adds or edits an example YAML
- **WHEN** a pull request changes files under `examples/**` and nothing else
- **THEN** the documentation CI check runs (the path filter includes `examples/**`), and an unregistered example fails the gate.
