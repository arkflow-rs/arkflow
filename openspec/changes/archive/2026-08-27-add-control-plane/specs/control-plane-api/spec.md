## ADDED Requirements

### Requirement: Unified control HTTP server
The system SHALL serve health checks and control-plane routes from one configurable HTTP server and SHALL report listener binding failures to the Engine startup path.

#### Scenario: Server starts with configured address
- **WHEN** the Engine starts with control HTTP enabled and an available address
- **THEN** health routes and `/api/v1` routes are reachable on that address

#### Scenario: Server bind fails
- **WHEN** the configured address cannot be bound
- **THEN** Engine startup returns an error instead of reporting readiness or panicking in a detached task

### Requirement: Versioned system and Stream APIs
The system SHALL expose `GET /api/v1/system`, `GET /api/v1/status`, `GET /api/v1/streams`, and `GET /api/v1/streams/{id}` with JSON responses containing stable Stream IDs, lifecycle state, timestamps, errors, and available metrics.

#### Scenario: List running Streams
- **WHEN** a client requests `GET /api/v1/streams`
- **THEN** the response includes every configured Stream exactly once with its current state

#### Scenario: Unknown Stream
- **WHEN** a client requests a Stream ID that is not registered
- **THEN** the API returns HTTP 404 with the standard error envelope

### Requirement: Component and schema discovery
The system SHALL expose registered component metadata and the generated full Engine configuration JSON Schema through versioned API endpoints.

#### Scenario: Discover components
- **WHEN** a client requests the component catalogue
- **THEN** the response groups registered input, output, processor, buffer, and codec metadata with descriptions, schemas, and examples when available

#### Scenario: Retrieve configuration schema
- **WHEN** a client requests the Engine schema
- **THEN** the response contains the same registered-component-aware schema used by the CLI schema command

### Requirement: Standard API errors
Control API failures SHALL use a consistent JSON error envelope containing an error code, human-readable message, and optional field or Stream ID context.

#### Scenario: Invalid request
- **WHEN** a client submits an invalid Stream ID or malformed request body
- **THEN** the response contains a non-2xx status and the standard error envelope
