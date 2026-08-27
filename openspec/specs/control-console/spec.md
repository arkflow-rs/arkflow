# control-console Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Console system dashboard
The Web Console SHALL display Engine state, Stream counts by lifecycle state, aggregate activity metrics, and recent errors using the Control API.

#### Scenario: Open dashboard
- **WHEN** the operator opens the console against a reachable Engine
- **THEN** the dashboard renders current system and Stream summary data and indicates API errors clearly

### Requirement: Stream inspection and controls
The Web Console SHALL provide Stream list and detail views with topology summary, state, metrics, recent errors, and start/stop/restart actions.

#### Scenario: Restart from Stream detail
- **WHEN** the operator confirms a restart action
- **THEN** the console submits the versioned API command, shows the operation state, and refreshes the Stream status

### Requirement: Schema-driven configuration editing
The Web Console SHALL provide configuration text editing and SHALL use the API-provided JSON Schema and component metadata for validation, completion, and component guidance.

#### Scenario: Invalid configuration feedback
- **WHEN** the operator edits an invalid configuration and requests validation
- **THEN** the console displays structured validation errors with their configuration paths and does not offer a publish action as successful

### Requirement: Configuration publishing and rollback
The Web Console SHALL allow an operator to inspect configuration versions, review validation results, publish a valid configuration, and request rollback to a prior version.

#### Scenario: Publish valid configuration
- **WHEN** the operator submits a validated configuration
- **THEN** the console displays the API application result, affected Streams, and any recovery failure

### Requirement: Safe display of secrets
The Web Console SHALL render redacted values returned by the API and SHALL NOT expose plaintext credentials in browser logs, URLs, or client-side error messages.

#### Scenario: View configured connector credentials
- **WHEN** the operator opens a connector configuration containing credentials
- **THEN** the console displays redaction markers rather than secret values

