## ADDED Requirements

### Requirement: Durable node maintenance mode

The service SHALL expose authenticated node maintenance actions that persist one of `active`, `draining`, or `maintenance`, append an audit event with actor and correlation metadata, and return the durable node representation. Reconciliation SHALL NOT dispatch new Attempts to a node in `draining` or `maintenance`, while already dispatched Attempts MAY settle.

#### Scenario: Drain an active node
- **WHEN** an authorized operator posts a drain action for an active node
- **THEN** the service persists `draining`, emits a `node_maintenance_changed` event, and prevents new command dispatches while preserving desired state

#### Scenario: Enter maintenance mode
- **WHEN** an authorized operator posts a maintenance action
- **THEN** the service persists `maintenance`, exposes the mode in node/status resources, and records the actor and correlation ID

#### Scenario: Resume a node
- **WHEN** an authorized operator deletes the node maintenance action
- **THEN** the service persists `active`, emits an audit event, and makes eligible current-generation Intents dispatchable again

#### Scenario: Reject an unauthorized maintenance action
- **WHEN** a request without valid operator authorization attempts to drain or maintain a node
- **THEN** the service returns `401` or `403` and does not mutate the node or append an audit event

### Requirement: Operational audit events

Operational mutations SHALL be represented in the same ordered event resource as reconciliation events, with event type, node identity, actor, correlation ID, previous mode, new mode, and outcome. Event payloads SHALL exclude authorization values and secret configuration content.

#### Scenario: Audit a drain transition
- **WHEN** a drain action succeeds
- **THEN** an ordered event is queryable by node and correlation ID and identifies the mode transition and successful outcome

#### Scenario: Audit a failed transition
- **WHEN** a maintenance action fails because the node does not exist or storage is unavailable
- **THEN** the API returns a stable problem and does not emit a successful state-transition event
