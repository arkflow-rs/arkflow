# job-operator-contracts Specification

## Purpose

Created by syncing the `clarify-job-operator-contracts` change. Defines how Job validation treats distributed Join plans, how configured state namespaces map to runtime storage, and how state byte budgets are validated and enforced.

## Requirements

### Requirement: Unsupported distributed Join plans SHALL be rejected

The Job validator SHALL reject an `OperatorKind::Join` plan unless the selected runtime explicitly advertises a multi-input Join implementation with keyed state, watermark, barrier, and checkpoint semantics. A single-input Processor builder MUST NOT be used as an implicit Join implementation.

#### Scenario: Public Job uses Join without a multi-input runtime

- **WHEN** validation encounters a distributed Job operator with kind `join` and no dedicated multi-input implementation
- **THEN** validation fails with an actionable unsupported-Join error before persistence or deployment

#### Scenario: Supported single-input operators remain valid

- **WHEN** validation encounters map, filter, aggregate, window, or UDF operators with supported builders
- **THEN** those operators continue through their existing validation and graph construction paths

### Requirement: Configured state namespaces SHALL affect runtime storage

Every stateful/window operator SHALL use one effective namespace containing Job identity, the configured namespace prefix or a default, operator identity, and task identity. Checkpoint snapshot and restore SHALL use the same effective namespace.

#### Scenario: Two configured namespace prefixes are isolated

- **WHEN** two stateful Jobs configure equal operator and task ids but different namespace prefixes
- **THEN** their backend entries and checkpoint snapshots remain distinct

#### Scenario: Namespace is restored consistently

- **WHEN** a checkpoint is restored for a stateful operator with a configured namespace
- **THEN** the runtime reads and writes the same effective namespace used when the checkpoint was created

### Requirement: State byte budgets SHALL be validated and enforced

`state.max_bytes`, when configured, SHALL be positive and SHALL be passed to the configured disk-backed state backend. A write that would exceed the live-byte budget SHALL fail without silently dropping state or advancing the source acknowledgement.

#### Scenario: Zero byte budget is rejected

- **WHEN** a Job declares `state.max_bytes: 0`
- **THEN** Job validation rejects it before deployment

#### Scenario: Write exceeds the configured budget

- **WHEN** a state mutation would exceed `state.max_bytes`
- **THEN** the backend returns a budget error, the processing unit remains replayable, and state is not partially committed

#### Scenario: Write reaches the budget exactly

- **WHEN** a state mutation brings live state exactly to `state.max_bytes`
- **THEN** the mutation succeeds and the next exceeding mutation fails
