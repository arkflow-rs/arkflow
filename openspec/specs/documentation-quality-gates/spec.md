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

### Requirement: Code-to-inventory freshness SHALL be enforced by the Rust test suite
The workspace test run SHALL include a snapshot test that compares the
committed documentation inventory (`docs/reference/component-inventory.json`)
and the published config-schema asset (`docs/static/config-schema.json`)
against the live registry and schema. The test SHALL fail with the
regeneration command when either artifact is stale and SHALL NOT modify files
unless the explicit regeneration flag is set. The docs CI job SHALL NOT
require a Rust toolchain; the committed artifacts are the seam between the
two gates.

#### Scenario: Rust CI catches a registry change without regeneration
- **WHEN** a pull request changes component registration and runs
  `cargo test --workspace --all-targets`
- **THEN** the snapshot test fails naming the stale artifact and the
  regeneration command, blocking the pull request

#### Scenario: Docs CI stays Node-only
- **WHEN** the documentation check workflow runs
- **THEN** it performs inventory-to-page validation against the committed
  generated artifacts without compiling Rust

### Requirement: Example configurations SHALL be offline-validated
Every example registered in `docs/reference/example-manifest.json` SHALL be
deep-validated offline by a workspace test (parsing, stream/job graph checks,
and semantic configuration validation, matching `--validate` semantics).
Examples that cannot be validated offline SHALL carry an explicit
`validate: false` exclusion with a stated reason in the manifest; silent
skipping SHALL NOT be permitted.

#### Scenario: An example drifts from the config schema
- **WHEN** a configuration field is renamed or removed in the engine and the
  workspace tests run
- **THEN** the example validation test fails naming the affected example file
  and the validation error

#### Scenario: An example cannot be validated offline
- **WHEN** an example requires external state that offline validation cannot
  satisfy
- **THEN** its manifest entry records `validate: false` with a reason, and
  the validation test documents the exclusion rather than skipping silently

### Requirement: The example manifest SHALL cover every example YAML on disk
Every `*.yaml`/`*.yml` file directly under `examples/` SHALL be registered in
`docs/reference/example-manifest.json`; the documentation validation command
SHALL fail when a new example file is not registered, so examples cannot be
added without entering the offline-validation gate.

#### Scenario: A new example YAML is added without registration
- **WHEN** a contributor adds `examples/foo_example.yaml` without a manifest
  entry and the documentation validation command runs
- **THEN** the command fails naming `examples/foo_example.yaml` as
  unregistered

#### Scenario: An example cannot pass offline validation
- **WHEN** a registered example cannot be deep-validated offline (for
  instance a legacy configuration the compiler rejects by design)
- **THEN** its manifest entry records `validate: false` with a stated reason
  instead of leaving the file unregistered

### Requirement: Component builders and docs metadata SHALL be name-aligned
Each component kind SHALL expose its registered builder names, and the
workspace test run SHALL include a test that fails when a builder is
registered without matching docs metadata or docs metadata exists without a
registered builder, because either misalignment silently breaks
`components list`, the docs inventory, or the IDE schema.

#### Scenario: A plugin registers a builder without docs metadata
- **WHEN** a plugin's `init()` calls the builder registration but not the
  metadata registration, and the workspace tests run
- **THEN** the registry consistency test fails naming the kind and component

#### Scenario: Docs metadata exists without a builder
- **WHEN** metadata is registered for a component name that has no
  registered builder, and the workspace tests run
- **THEN** the registry consistency test fails naming the orphan metadata
  entry
