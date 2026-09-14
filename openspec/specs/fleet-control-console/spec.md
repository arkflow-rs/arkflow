# fleet-control-console Specification

## Purpose
TBD - created by archiving change make-control-plane-hub. Update Purpose after archive.
## Requirements
### Requirement: Fleet and node navigation

The console SHALL present node health and fleet summaries before node-scoped
Stream details, and SHALL persist the selected node in the current route/query
state.

#### Scenario: Select a node

- **WHEN** an operator selects a node from the fleet view
- **THEN** runtime, configuration, events, and operations views scope requests
  to that node and visibly show the target identity

#### Scenario: Stale node

- **WHEN** the Hub marks a node stale
- **THEN** the console shows stale status, last-seen time, and disables or
  explains unavailable mutating actions

### Requirement: Hub operation feedback

The console SHALL display target node, dispatch state, progress, correlation ID,
terminal result, and disconnect/conflict errors for Hub operations.

#### Scenario: Poll a node operation

- **WHEN** an operator starts or stops a Stream on a selected node
- **THEN** the console polls the Hub operation and updates the UI through
  queued, dispatched, running, and terminal states

### Requirement: Secret-safe multi-node administration

The console SHALL never render node credentials or unredacted configuration
secrets and SHALL distinguish Hub permission errors from node availability
errors.

#### Scenario: Permission denied

- **WHEN** a protected Hub command returns `401` or `403`
- **THEN** the console shows a permission state without retrying the mutation or
  exposing the bearer token

