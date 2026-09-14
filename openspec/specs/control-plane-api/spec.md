# control-plane-api Specification

## Purpose
TBD - created by archiving change add-control-plane. Update Purpose after archive.
## Requirements
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

Control API failures SHALL use a consistent JSON error envelope containing an error code, human-readable message, and optional field or Stream ID context. A failed conditional write, such as a generation or version conflict, SHALL return a non-2xx conflict status carrying the expected and observed values, and SHALL NOT be reported as a generic invalid request.

#### Scenario: Invalid request

- **WHEN** a client submits an invalid Stream ID or malformed request body
- **THEN** the response contains a non-2xx status and the standard error envelope

#### Scenario: Concurrent mutation conflicts with a conditional write

- **WHEN** a request carrying an expected generation targets a resource whose generation has since advanced
- **THEN** the response is a precondition-failed envelope naming the expected and observed generation, and the stored resource keeps the newer state

### Requirement: Job upgrade and rollback SHALL fence the whole written record

A Job upgrade or rollback SHALL write the Job through a conditional update that fences the generation the handler read, and SHALL NOT write the recovery/checkpoint pointer, which is owned by the checkpoint path: a checkpoint moves it without bumping the generation, so a handler copying its earlier read back would regress recovery to a pointer retention may already have deleted. The version and spec a rollback restores SHALL be written, and artifact selection SHALL filter recovery candidates by job version and state format so a pointer the restored version cannot use is reported instead of silently ignored.

#### Scenario: A checkpoint lands during a rollback

- **WHEN** a checkpoint report moves the Job's recovery pointer after a rollback handler read the Job and before it writes
- **THEN** the conditional write leaves the newer pointer untouched, and the Job's recovery selection never regresses to the pointer the handler read

#### Scenario: Concurrent desired-state change

- **WHEN** a desired-state change bumps the Job's generation between a rollback handler's read and its write
- **THEN** the write fails with a conflict, the newer desired state is kept, and the operator can retry against the fresh generation

### Requirement: Upgrade and rollback SHALL validate state compatibility equally

An upgrade and a rollback SHALL both validate the selected artifact's state format against the Job's declared state format before accepting the request, and SHALL reject an incompatible target with an explicit error. A rollback SHALL NOT be accepted when the target version's state format the Job cannot restore.

#### Scenario: Rollback to an incompatible state format

- **WHEN** an operator requests a rollback to a Job version whose declared state format differs from the artifact the Job would restore
- **THEN** the API rejects the request with the state-format incompatibility error and leaves the Job unchanged, instead of accepting it and degrading to a stateless restart

#### Scenario: Rollback to a compatible state format

- **WHEN** the requested target version declares the same state format as the Job's current artifact
- **THEN** the rollback is accepted and the Job's recovery selection uses the restored version's spec

