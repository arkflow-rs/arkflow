## MODIFIED Requirements

### Requirement: Node-targeted command dispatch
The Hub SHALL require a target node for mutating commands and SHALL track each command through queued, dispatched, acknowledged, running, and terminal states. Expired leased commands SHALL transition to a retryable state or remain available for redelivery; they SHALL not disappear while their operation remains an active deduplication record.

#### Scenario: Dispatch a lifecycle command
- **WHEN** an operator submits a start command for a selected node and Stream
- **THEN** the Hub returns an operation ID, queues an idempotent command for that node, and exposes dispatch/acknowledgement timestamps

#### Scenario: Target node is unavailable
- **WHEN** a command targets a stale or offline node
- **THEN** the Hub does not claim execution success and returns an operation with `node_unavailable` or a conflict problem according to the command contract

#### Scenario: A command lease expires
- **WHEN** an Agent does not complete a leased command before its lease expires
- **THEN** the operation becomes retryable and reconciliation can enqueue a replacement without being blocked by stale deduplication state

### Requirement: Command idempotency and reconciliation
The Hub and Agent SHALL use a stable command idempotency key containing the resource generation and SHALL reconcile in-flight operations when a node reconnects. Results for terminal operations or mismatched generations SHALL be ignored.

#### Scenario: Duplicate command delivery
- **WHEN** the Agent receives the same command more than once
- **THEN** it executes it at most once and returns the existing command result

#### Scenario: A late result arrives
- **WHEN** an Agent reports a result after the operation was cancelled or superseded
- **THEN** the Hub does not overwrite the terminal operation or revive the old Job generation
