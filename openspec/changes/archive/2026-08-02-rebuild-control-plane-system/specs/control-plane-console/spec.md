## ADDED Requirements

### Requirement: Operations application shell

The console SHALL provide persistent navigation and route-level pages for Overview, Runtime, Configuration, Components, Events, and Settings. It SHALL show global connection, permission, stale-data, loading, empty, and error states.

#### Scenario: Open the overview
- **WHEN** an operator opens the console
- **THEN** the console loads system identity, node health, runtime totals, active operations, recent events, and aggregate metrics in one overview

#### Scenario: API becomes unavailable
- **WHEN** the control API cannot be reached after data was loaded
- **THEN** the console marks data stale, preserves the last safe snapshot, and provides a retry action

### Requirement: Runtime administration

The Runtime page SHALL support filtering and pagination over Streams, topology/resource summaries, desired/observed state, metrics, recent errors, operation history, and lifecycle actions. Actions SHALL display operation progress and terminal results.

#### Scenario: Operate one Stream from the runtime page
- **WHEN** an operator starts, stops, or restarts a Stream
- **THEN** the UI confirms the action, tracks its operation ID, refreshes the affected resource, and reports success or failure without blocking other Streams

#### Scenario: Inspect a failed Stream
- **WHEN** an operator opens a failed Stream
- **THEN** the UI shows last error, transition timeline, metrics, recent related events, and available recovery actions

### Requirement: Configuration workflow

The Configuration page SHALL load redacted active configuration, support YAML/JSON editing, schema-aware validation with path locations, draft/publish separation, version listing, diff metadata, and rollback confirmation.

#### Scenario: Validate an invalid draft
- **WHEN** an operator validates a malformed or semantically invalid draft
- **THEN** the UI displays structured path-aware errors and does not publish or alter runtime state

#### Scenario: Publish and rollback configuration
- **WHEN** an operator publishes a valid draft or rolls back a version
- **THEN** the UI creates/tracks an operation, shows affected resources, and refreshes the active version only after the server reports success

### Requirement: Components, events, and settings

The console SHALL provide component catalogue/schema display, filterable event/audit history, and settings for API endpoint, authentication status, CORS/security posture, node identity, and capability information. It SHALL preserve redaction and never put secrets in URLs, browser logs, or client-side error messages.

#### Scenario: Investigate an administrative event
- **WHEN** an operator filters events by Stream, operation, type, or outcome
- **THEN** the UI shows ordered event details and links to the related resource/operation without exposing credentials

#### Scenario: View security settings
- **WHEN** an operator opens Settings
- **THEN** the console shows whether the session is authenticated and the server's effective security/capability status without rendering token values
