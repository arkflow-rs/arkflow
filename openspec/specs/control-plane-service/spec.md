# Purpose

Define the dedicated ArkFlow control-plane service boundary, resource API, runtime operations, and transport security behavior.

# Requirements

### Requirement: Dedicated control-plane service boundary

The system SHALL expose control-plane HTTP transport from arkflow-server through a domain facade, and arkflow-core::Engine SHALL remain usable without constructing an HTTP Router. Health, readiness, and liveness endpoints SHALL remain available for compatibility.

#### Scenario: Start the control-plane service
- **WHEN** the ArkFlow binary starts with control-plane settings enabled
- **THEN** it starts the Engine domain and the dedicated server using one configured listener

#### Scenario: Embed the Engine without HTTP
- **WHEN** a caller constructs and runs the Engine domain in a library context
- **THEN** no Axum Router or listener is required

### Requirement: Resource-oriented system API

The API SHALL expose versioned resources for system identity, runtime nodes, Streams, components, configuration, operations, events, and metrics. Collection responses SHALL provide stable envelopes and pagination metadata where applicable.

#### Scenario: Discover the control-plane resources
- **WHEN** a client requests /api/v1/system and /api/v1/nodes
- **THEN** it receives node identity, version, capabilities, health state, and runtime summary

#### Scenario: List many Streams
- **WHEN** a client requests /api/v1/streams?page=2&page_size=20
- **THEN** it receives only the requested page plus total and page metadata

### Requirement: Desired and observed lifecycle state

Each Stream resource SHALL expose desired state, observed state, transition timestamps, active operation ID, metrics, and bounded recent errors. Lifecycle commands SHALL be idempotent and SHALL return an operation representation.

#### Scenario: Start a stopped Stream
- **WHEN** a client posts a start command for a stopped Stream
- **THEN** the API returns an operation ID and the Stream transitions from stopped to starting to running

#### Scenario: Repeat an active command
- **WHEN** a client repeats the same lifecycle command while an equivalent operation is active
- **THEN** the API returns the existing operation or a conflict response without spawning a duplicate task

### Requirement: Operations and events

The service SHALL retain bounded operation records and ordered control events with operation ID, resource ID, timestamp, actor/correlation ID, outcome, and error details. Clients SHALL be able to query an operation and list events.

#### Scenario: Observe a completed operation
- **WHEN** a client requests /api/v1/operations/{id} after a restart operation
- **THEN** it receives terminal status, start/end timestamps, affected resource, and resulting observed state

#### Scenario: Correlate an administrative request
- **WHEN** a client sends an X-Correlation-ID
- **THEN** the response, operation record, event, and structured access log carry the same correlation ID

### Requirement: Standard transport and security behavior

The server SHALL apply authentication, CORS policy, request correlation, request-size limits, and a consistent JSON problem envelope at the outer router. It SHALL NOT log request bodies, authorization values, or secret configuration values.

#### Scenario: Reject an unauthenticated write
- **WHEN** a protected lifecycle or configuration write lacks valid credentials
- **THEN** the service returns a problem response with 401 or 403 and does not create an operation

#### Scenario: Reject an invalid resource request
- **WHEN** a client requests an unknown resource or invalid page/filter
- **THEN** the service returns a stable problem code, human-readable message, correlation ID, and appropriate HTTP status
