## MODIFIED Requirements

### Requirement: Configuration validation

The system SHALL validate candidate configuration syntax, Stream and Job identity constraints, component configuration, state backend construction, and Stream/Job graph construction before applying it. Validation SHALL not merely call `JobSpec::validate`; it SHALL exercise the same side-effect-free deep-build path used before runtime startup. The validation endpoint SHALL require the same operator authorization as configuration application: an unauthenticated request SHALL be rejected with 401 and SHALL NOT trigger secret-reference resolution, component construction, or any other validation work on the submitted content.

#### Scenario: Valid candidate

- **WHEN** a client submits a syntactically valid configuration whose Streams, Jobs, components, and local backends can be built
- **THEN** validation returns success and identifies the candidate as applicable

#### Scenario: Invalid candidate

- **WHEN** a candidate has malformed configuration, duplicate Stream or Job ID, an unknown component, an unsupported backend, an invalid graph, or invalid WAL settings
- **THEN** validation returns structured errors and does not change the running configuration

#### Scenario: Unauthenticated validation is rejected

- **WHEN** a client submits a candidate to the validation endpoint without a valid operator credential while the control plane requires authorization
- **THEN** the endpoint returns 401 and performs no parsing, secret-reference resolution, or component construction on the submitted content
