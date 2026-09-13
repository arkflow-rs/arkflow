# control-plane-hub Specification

## Purpose
TBD - created by archiving change make-control-plane-hub. Update Purpose after archive.
## Requirements
### Requirement: Hub node registry

The Hub SHALL maintain a registry keyed by stable `node_id`, including node
capabilities, current status, last heartbeat, lease expiry, and the latest
reported system/runtime snapshot.

#### Scenario: Node registers

- **WHEN** a compute node presents a valid node identity and registration token
- **THEN** the Hub creates or resumes that node record and returns an
  authenticated session token plus the current report contract version

#### Scenario: Node lease expires

- **WHEN** a node misses heartbeats beyond its configured lease TTL
- **THEN** the Hub marks it stale and excludes it from healthy-node counts while
  retaining its last-known resources with a stale indicator

### Requirement: Agent report ingestion

The Hub SHALL accept authenticated heartbeat and full/delta report messages for
system identity, Streams, operations, events, and metrics, and SHALL reject
reports from unknown or invalid sessions.

#### Scenario: Node reports runtime state

- **WHEN** an authenticated node posts a report containing Stream snapshots
- **THEN** the Hub updates only that node's resources, records the report time,
  and makes the resources available to aggregated queries

#### Scenario: Invalid agent session

- **WHEN** a report uses an expired or mismatched session token
- **THEN** the Hub returns `401` with a stable problem code and does not mutate
  the node registry

### Requirement: Aggregated and targeted resources

The Hub SHALL aggregate node resources for read APIs and SHALL preserve
`node_id` on every node-owned Stream, operation, event, and metric resource.

#### Scenario: List fleet Streams

- **WHEN** an operator requests the Hub Stream collection without a node filter
- **THEN** the response contains resources from all registered nodes with
  stable pagination and node identity fields

#### Scenario: Resolve duplicate Stream IDs

- **WHEN** two nodes report a Stream with the same local ID
- **THEN** the Hub exposes two distinct resources keyed by `(node_id, stream_id)`
  and never routes a command by local ID alone

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

### Requirement: Job operation expiry and bounded retry

Job operations tracked in the durable operation record SHALL carry an expiry timestamp and a retry count. A non-terminal `job_start` or `job_stop` operation whose delivery window expires SHALL be transitioned by the periodic sweep to a retryable terminal state with its retry count incremented; reconciliation SHALL re-enqueue such operations with the accumulated retry count, and an operation whose retry count reaches the cap SHALL settle as terminal failed with failure classification `expired` and no further re-enqueue. Checkpoint and savepoint trigger operations carry the same expiry metadata, and their delivery-window expiry remains governed by the command-lease path, which replays the original trigger payload. Expired operations SHALL NOT be deleted while they remain the active deduplication record for their (Job, generation, operation).

#### Scenario: An expired start becomes retryable

- **WHEN** a queued or dispatched `job_start` or `job_stop` operation passes its expiry timestamp without reaching a satisfying terminal state
- **THEN** the periodic sweep marks it retryable with an incremented retry count, drops the undeliverable command, and reconciliation re-enqueues the desired-state operation

#### Scenario: Retry cap reached

- **WHEN** an expired lifecycle operation's retry count reaches the cap
- **THEN** the operation reaches a terminal failed state with failure classification `expired` and reconciliation stops re-enqueueing it

#### Scenario: A checkpoint trigger expiry replays its payload

- **WHEN** a dispatched checkpoint or savepoint trigger outlives its command lease
- **THEN** the command-lease path marks the operation retryable and re-enqueues the original trigger payload, preserving the recovery-artifact semantics

#### Scenario: Older records without expiry metadata keep working

- **WHEN** the Hub opens a database created before this change
- **THEN** existing operation rows gain default expiry metadata without behavioral change and remain queryable and deduplicated
