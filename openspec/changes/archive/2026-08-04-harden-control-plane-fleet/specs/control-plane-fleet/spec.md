## ADDED Requirements

### Requirement: Durable Fleet resource representation

The Hub SHALL expose Node, Stream, configuration version, Operation, Audit Event, and Rollout resources with stable identifiers and explicit desired, observed, and convergence data where applicable.

#### Scenario: Read a Fleet resource after Hub restart
- **WHEN** an operator requests a node, Stream, operation, or rollout after the Hub has restarted
- **THEN** the response contains the same durable identity, state, generation, and latest outcome that existed before restart

### Requirement: Agent observation integrity

The Hub SHALL accept Agent observations only from the authenticated session and SHALL reject or ignore reports with an older boot identity or report sequence without changing the current observed snapshot.

#### Scenario: Reject a stale observation
- **WHEN** an Agent submits a report older than the stored boot and sequence cursor
- **THEN** the Hub acknowledges the request without regressing observed state, metrics, or configuration version

### Requirement: Node compatibility status

The Hub SHALL record each node's protocol version, software version, and capabilities and SHALL make compatibility status visible before dispatching a command that requires capabilities.

#### Scenario: Block an incompatible command
- **WHEN** a rollout targets a node that lacks a required capability or protocol version
- **THEN** the Hub does not dispatch an executable Attempt and records a stable compatibility failure for that node

### Requirement: Durable operational history

The Hub SHALL retain bounded queryable operation and audit history according to configured retention limits, without removing the current desired state or latest observed state when old history is pruned.

#### Scenario: Prune old history
- **WHEN** operation or audit history exceeds its configured retention bound
- **THEN** the Hub prunes only eligible historical records and continues serving current resource state and active intents
