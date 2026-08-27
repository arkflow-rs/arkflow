# configuration-management Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
### Requirement: Configuration validation
The system SHALL validate candidate configuration syntax, Stream identity constraints, component configuration, and Stream construction before applying it.

#### Scenario: Valid candidate
- **WHEN** a client submits a syntactically valid configuration whose components can be built
- **THEN** validation returns success and identifies the candidate as applicable

#### Scenario: Invalid candidate
- **WHEN** a candidate has malformed configuration, an unknown component, duplicate Stream ID, or invalid WAL settings
- **THEN** validation returns structured errors and does not change the running configuration

### Requirement: Versioned configuration application
The system SHALL persist successful configuration versions and SHALL apply only the Streams affected by a configuration change.

#### Scenario: Add a Stream
- **WHEN** a valid configuration adds a new Stream ID
- **THEN** the new Stream is built and started while unchanged Streams remain running

#### Scenario: Change a Stream
- **WHEN** a valid configuration changes an existing Stream
- **THEN** the old instance is replaced through controlled stop and start, and the configuration version records the change

### Requirement: Failed application recovery
The system SHALL preserve the last known-good configuration and SHALL report a failed application without silently discarding the previous version.

#### Scenario: Candidate build fails
- **WHEN** a candidate Stream cannot be built
- **THEN** the old configuration remains active and the response identifies the build failure

#### Scenario: Startup after replacement fails
- **WHEN** a replacement Stream fails to start after the old instance was stopped
- **THEN** the system attempts to restore the old instance and reports whether restoration succeeded

### Requirement: Configuration rollback
The system SHALL allow an operator to select a previously successful configuration version and apply it using the same validation and recovery rules as a new configuration.

#### Scenario: Roll back to a prior version
- **WHEN** an operator requests rollback to an existing version
- **THEN** the selected version is validated and applied, and a new current-version record identifies the rollback operation

### Requirement: Secret redaction
Configuration read APIs and diagnostic responses SHALL redact configured credential, token, password, and secret fields by default.

#### Scenario: Read sensitive configuration
- **WHEN** a client requests the current configuration
- **THEN** sensitive values are replaced by a redaction marker and are not returned in plaintext

