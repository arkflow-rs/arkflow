## MODIFIED Requirements

### Requirement: Configuration validation
The system SHALL validate candidate configuration syntax, Stream and Job identity constraints, component configuration, state backend construction, and Stream/Job graph construction before applying it. Validation SHALL not merely call `JobSpec::validate`; it SHALL exercise the same side-effect-free deep-build path used before runtime startup.

#### Scenario: Valid candidate
- **WHEN** a client submits a syntactically valid configuration whose Streams, Jobs, components, and local backends can be built
- **THEN** validation returns success and identifies the candidate as applicable

#### Scenario: Invalid candidate
- **WHEN** a candidate has malformed configuration, duplicate Stream or Job ID, an unknown component, an unsupported backend, an invalid graph, or invalid WAL settings
- **THEN** validation returns structured errors and does not change the running configuration

### Requirement: Failed application recovery
The system SHALL preserve the last known-good configuration and SHALL report a failed application without silently discarding the previous version. A dry-run, graph-build, resource-connect, or immediate local Job startup failure SHALL transition the affected runtime to `Failed` before the error is returned.

#### Scenario: Candidate build fails
- **WHEN** a candidate Stream or Job cannot be built
- **THEN** the old configuration remains active and the response identifies the build failure

#### Scenario: Startup after replacement fails
- **WHEN** a replacement Stream or Job fails to start after the old instance was stopped
- **THEN** the system attempts to restore the old instance, marks the failed replacement as `Failed`, and reports whether restoration succeeded

## ADDED Requirements

### Requirement: Validation resources SHALL be released before real startup
The configuration validator SHALL close any temporary WAL, temporary component, state backend, and connector handle created during deep validation before the candidate is rebuilt for actual execution.

#### Scenario: Durable candidate is validated and started
- **WHEN** a durability-enabled candidate passes dry-run validation and is immediately started
- **THEN** the validation WAL has been flushed and closed, the real runtime can reopen the same path, and no exclusive-lock failure is caused by the validator
