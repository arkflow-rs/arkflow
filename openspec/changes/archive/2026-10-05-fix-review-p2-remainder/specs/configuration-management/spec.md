# configuration-management Delta

## MODIFIED Requirements

### Requirement: Versioned configuration application
The system SHALL persist successful configuration versions and SHALL apply only the Streams affected by a configuration change. A configuration version SHALL be retained as a rollback point only after its application succeeded: when validation or application fails after the candidate version was persisted, the system SHALL remove that never-effective version (best-effort compensating delete) before returning the error, so the version history MUST NOT accumulate versions that were never active. Rollback SHALL follow the same rule. The version history directory SHALL be overridable by the `ARKFLOW_CONFIG_HISTORY_DIR` environment variable (default `.arkflow/config-history` unchanged).

#### Scenario: Add a Stream
- **WHEN** a valid configuration adds a new Stream ID
- **THEN** the new Stream is built and started while unchanged Streams remain running

#### Scenario: Change a Stream
- **WHEN** a valid configuration changes an existing Stream
- **THEN** the old instance is replaced through controlled stop and start, and the configuration version records the change

#### Scenario: Failed application leaves no never-effective version
- **WHEN** a candidate configuration is persisted and the subsequent runtime application fails
- **THEN** the candidate version is removed by a best-effort compensating delete and the error is returned, leaving the version history containing only versions that were active

#### Scenario: History directory override
- **WHEN** the Hub process starts with `ARKFLOW_CONFIG_HISTORY_DIR` set to a writable path
- **THEN** configuration versions are stored under that path instead of the default relative directory
