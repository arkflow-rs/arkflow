## MODIFIED Requirements

### Requirement: Resource-oriented system API

The API SHALL expose versioned resources for system identity, runtime nodes, Streams, components, configuration, operations, events, and metrics. Collection responses SHALL provide stable envelopes and pagination metadata where applicable. `GET /api/v1/nodes` SHALL return a paginated node collection in both local and Hub mode, and every node-owned resource SHALL preserve `node_id`. Metrics SHALL be reachable under the same configured versioned prefix as the other control resources.

#### Scenario: Discover the control-plane resources
- **WHEN** a client requests /api/v1/system and /api/v1/nodes
- **THEN** it receives node identity, version, capabilities, health state, and runtime summary in stable resource/envelope shapes

#### Scenario: List many Streams
- **WHEN** a client requests /api/v1/streams?page=2&page_size=20
- **THEN** it receives only the requested page plus total and page metadata

#### Scenario: Read local nodes as a collection
- **WHEN** a client requests /api/v1/nodes against a local Engine
- **THEN** it receives one local node resource inside a paginated collection, rather than a singleton response

#### Scenario: Read metrics through the versioned API
- **WHEN** a client requests /api/v1/metrics
- **THEN** it receives aggregate and per-node metrics using the same authentication, correlation, and routing rules as other control resources

### Requirement: Operations and events

The service SHALL retain bounded operation records and ordered control events with operation ID, resource ID, timestamp, actor/correlation ID, outcome, and error details. Clients SHALL be able to query an operation and list events. Hub operations SHALL include target node, dispatch/acknowledgement timestamps, progress, correlation ID, and terminal availability or permission errors.

#### Scenario: Observe a completed operation
- **WHEN** a client requests /api/v1/operations/{id} after a restart operation
- **THEN** it receives terminal status, start/end timestamps, affected resource, and resulting observed state

#### Scenario: Correlate an administrative request
- **WHEN** a client sends an X-Correlation-ID
- **THEN** the response, operation record, event, and structured access log carry the same correlation ID

#### Scenario: Target an unavailable node
- **WHEN** a Hub command targets a stale or offline node
- **THEN** the service returns a non-success operation state with a stable availability problem and does not claim execution success

### Requirement: Standard transport and security behavior

The server SHALL apply authentication, CORS policy, request correlation, request-size limits, and a consistent JSON problem envelope at the outer router. It SHALL NOT log request bodies, authorization values, or secret configuration values. Compatibility aliases MAY remain, but the versioned resource routes SHALL be the canonical contract.

#### Scenario: Reject an unauthenticated write
- **WHEN** a protected lifecycle or configuration write lacks valid credentials
- **THEN** the service returns a problem response with 401 or 403 and does not create an operation

#### Scenario: Reject an invalid resource request
- **WHEN** a client requests an unknown resource or invalid page/filter
- **THEN** the service returns a stable problem code, human-readable message, correlation ID, and appropriate HTTP status
