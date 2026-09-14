# component-registry-export Delta

## ADDED Requirements

### Requirement: The component registry SHALL be machine-readable from the CLI
The engine SHALL expose the full component registry — every registered
component's kind, type-name, description, optional-config flag, configuration
JSON Schema, and example configuration — through `arkflow components list
--format json`. The same serialization function SHALL back the CLI output, the
committed documentation inventory, and the snapshot test, so the three cannot
diverge. Output SHALL be deterministic (sorted by kind then name, stable field
order, trailing newline).

#### Scenario: A user lists components as JSON
- **WHEN** `arkflow components list --format json` runs
- **THEN** it prints one JSON document containing every registered component
  with its kind, name, description, `config_optional`, `config_schema`, and
  `config_example` when present, sorted by kind then name

#### Scenario: Temporary components are listed
- **WHEN** the JSON or text listing is produced after all plugin `init()`
  functions run
- **THEN** components registered as temporary (e.g. `redis`) appear with kind
  `temporary`, indistinguishable in structure from other kinds

### Requirement: Temporary SHALL be a first-class component kind
`ComponentKind` SHALL include `temporary` as a sixth kind with the same
registration, listing, metadata, and schema treatment as the existing five
kinds. The plugin-local temporary builder registry SHALL be removed;
temporary components register through the core registry. The engine
configuration JSON Schema and `components` CLI output SHALL include the
temporary kind.

#### Scenario: A temporary component registers through core
- **WHEN** the temporary plugin `init()` runs
- **THEN** its builders and metadata live in the core registry, and the
  plugin-local registry no longer exists

#### Scenario: Schema and listings include temporary
- **WHEN** `arkflow components list` or `arkflow schema` output is produced
- **THEN** the temporary kind and its components are discoverable through the
  same interface as every other kind

### Requirement: The committed documentation inventory SHALL be a generated artifact
`docs/reference/component-inventory.json` SHALL be generated from the
registry export (format version 2, without a hand-maintained page-mapping
field) and SHALL carry a version marker. A Rust snapshot test SHALL fail when
the committed file differs from the live registry dump, and the failure
message SHALL state the exact regeneration command. The test SHALL NOT write
files unless the documented regeneration flag is set.

#### Scenario: A component is added without regenerating
- **WHEN** a plugin registers a new component and `cargo test --workspace`
  runs without the committed inventory being regenerated
- **THEN** the snapshot test fails, naming the stale file and printing the
  regeneration command

#### Scenario: A contributor regenerates
- **WHEN** the documented regeneration command runs with the regeneration
  flag set
- **THEN** the committed inventory is rewritten from the live registry and
  the snapshot test passes on the next run

### Requirement: The engine configuration JSON Schema SHALL be published as a docs asset
The output of the engine config schema command SHALL be committed as
`docs/static/config-schema.json` and linked from a documentation page that
explains IDE auto-completion setup. The same snapshot mechanism as the
component inventory SHALL enforce its freshness.

#### Scenario: The schema changes
- **WHEN** the engine configuration schema changes and the snapshot test runs
- **THEN** the test fails until `docs/static/config-schema.json` is
  regenerated from the live schema

#### Scenario: A reader wants IDE completion
- **WHEN** a reader opens the configuration reference
- **THEN** the page links to the published JSON Schema asset and documents
  how to enable editor auto-completion with it
